#include <cstring>
#include <algorithm>
#include <stdexcept>
#include "compression.hpp"

#if defined(HAVE_LZ4)
#include <lz4.h>

namespace cass {
class LZ4Compressor final : public ICompressor {
  virtual Buffer decompress(const Buffer& buffer) const {
    const char* in_data = buffer.data();
    size_t in_size = buffer.size();

    // cql sends uncompressed size in the first 4 bytes (bigendian)
    if (in_size < 4) {
      throw std::runtime_error("lz4: incomplete payload");
    }
    const unsigned char* size_ptr = reinterpret_cast<const unsigned char*>(in_data);
    size_t out_size
        = (size_ptr[0] << 24)
        | (size_ptr[1] << 16)
        | (size_ptr[2] << 8)
        | (size_ptr[3]);
    in_data += 4;
    in_size -= 4;

    Buffer new_buf = Buffer(RefBuffer::Ptr(RefBuffer::create(out_size)), out_size);
    char* out_data = new_buf.data();

    if (LZ4_decompress_safe(in_data, out_data, in_size, out_size) < 0) {
      throw std::runtime_error("lz4: failed to decompress");
    }
    return new_buf;
  };

  virtual Buffer compress(const cass::Buffer& buffer) const {
    size_t in_size = buffer.size();
    size_t out_size = 4+LZ4_compressBound(in_size);
    RefBuffer::Ptr compressed = RefBuffer::Ptr(RefBuffer::create(out_size));
    char* compressed_ptr = compressed->data();
    compressed_ptr[0] = (in_size >> 24) & 0xFF;
    compressed_ptr[1] = (in_size >> 16) & 0xFF;
    compressed_ptr[2] = (in_size >> 8) & 0xFF;
    compressed_ptr[3] = (in_size) & 0xFF;

    size_t compressed_size = LZ4_compress(buffer.data(), compressed_ptr + 4, in_size);
    if(compressed_size == 0) {
      throw std::runtime_error("lz4: failed to compress");
    }

    return Buffer(compressed, 4+compressed_size);
  }

  virtual const char* get_method_name() const {
    return "lz4";
  };
};
}

#endif

#if defined(HAVE_SNAPPY)
#include <snappy-c.h>

namespace cass {
class SnappyCompressor final : public ICompressor {
  virtual Buffer decompress(const Buffer& buffer) const {
    const char* in_data = buffer.data();
    size_t in_size = buffer.size();
    size_t out_size;
    if (snappy_uncompressed_length(buffer.data(), in_size, &out_size) != SNAPPY_OK) {
      throw std::runtime_error("snappy: unknown uncompressed size");
    }

    Buffer new_buf = Buffer(RefBuffer::Ptr(RefBuffer::create(out_size)), out_size);
    char* out_data = new_buf.data();

    if (snappy_uncompress(in_data, in_size, out_data, &out_size) != SNAPPY_OK) {
      throw std::runtime_error("snappy: failed to decompress");
    }

    return new_buf;
  };

  virtual Buffer compress(const cass::Buffer& buffer) const {
    const char* in_data = buffer.data();
    size_t in_size = buffer.size();
    size_t compressed_size = snappy_max_compressed_length(in_size);
    Buffer compressed = Buffer(compressed_size);
    char* compressed_ptr = compressed.data();
    if (snappy_compress(in_data, in_size, compressed_ptr, &compressed_size) != SNAPPY_OK) {
      throw std::runtime_error("snappy: failed to compress");
    }
    return Buffer(compressed.get_buffer(), compressed_size);
  }

  virtual const char* get_method_name() const {
    return "snappy";
  };
};
}

#endif

namespace cass {
ICompressor::Ptr get_compressor(const std::list<std::string>& methods,
        CassCqlCompression user_preference)
{
  // keep lz4 before snappy to make it preferred
#if defined(HAVE_LZ4)
  bool user_choice_lz4 = user_preference == CASS_CQL_COMPRESSION_ENABLE
                      || user_preference == CASS_CQL_COMPRESSION_LZ4;
  bool server_allowed_lz4 =
      std::find(methods.begin(), methods.end(), "lz4") != methods.end();

  if (user_choice_lz4 && server_allowed_lz4) {
    return ICompressor::Ptr(new LZ4Compressor);
  }
#endif

#if defined(HAVE_SNAPPY)
  bool user_choice_snappy = user_preference == CASS_CQL_COMPRESSION_ENABLE
                         || user_preference == CASS_CQL_COMPRESSION_SNAPPY;
  bool server_allowed_snappy =
      std::find(methods.begin(), methods.end(), "snappy") != methods.end();

  if (user_choice_snappy && server_allowed_snappy) {
    return ICompressor::Ptr(new SnappyCompressor);
  }
#endif

  return ICompressor::Ptr();
}

} // namespace cass
