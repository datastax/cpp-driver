#include <cstring>
#include <algorithm>
#include <stdexcept>
#include "compression.hpp"

#if defined(HAVE_LZ4)
#include <lz4.h>

namespace cass {
class LZ4Compressor final : public ICompressor {
  virtual Buffer decompress(const Buffer& buffer) const {
    char* in_data = buffer.data();
    size_t in_size = buffer.size();

    // cql sends uncompressed size in the first 4 bytes (bigendian)
    if (in_size < 4) {
      throw std::runtime_error("lz4: incomplete payload");
    }
    unsigned char* size_ptr = reinterpret_cast<unsigned char*>(in_data);
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
    char* in_data = buffer.data();
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

  virtual const char* get_method_name() const {
    return "snappy";
  };
};
}

#endif

namespace cass {

class IdentityCompressor final : public ICompressor {
  virtual Buffer decompress(const Buffer& buffer) const {
    return buffer;
  };

  virtual const char* get_method_name() const {
    return "";
  };
};

SharedRefPtr<ICompressor> get_compressor(const std::list<std::string>& methods,
        CassCqlCompression user_preference)
{
  // keep lz4 before snappy to make it preferred
#if defined(HAVE_LZ4)
  bool user_choice_lz4 = user_preference == CASS_CQL_COMPRESSION_ENABLE
                      || user_preference == CASS_CQL_COMPRESSION_LZ4;
  bool server_allowed_lz4 =
      std::find(methods.begin(), methods.end(), "lz4") != methods.end();

  if (user_choice_lz4 && server_allowed_lz4) {
    return SharedRefPtr<ICompressor>(new LZ4Compressor);
  }
#endif

#if defined(HAVE_SNAPPY)
  bool user_choice_snappy = user_preference == CASS_CQL_COMPRESSION_ENABLE
                         || user_preference == CASS_CQL_COMPRESSION_SNAPPY;
  bool server_allowed_snappy =
      std::find(methods.begin(), methods.end(), "snappy") != methods.end();

  if (user_choice_snappy && server_allowed_snappy) {
    return SharedRefPtr<ICompressor>(new SnappyCompressor);
  }
#endif

  return SharedRefPtr<ICompressor>(new IdentityCompressor);
}

} // namespace cass
