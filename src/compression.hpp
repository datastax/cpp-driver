#ifndef __CASS_COMPRESSION_HPP_INCLUDED__
#define __CASS_COMPRESSION_HPP_INCLUDED__

#include <string>
#include <list>
#include <utility>
#include "ref_counted.hpp"
#include "cassandra.h"

namespace cass {

class ICompressor : public RefCounted<ICompressor> {
public:
  struct Buffer {
  private:
      RefBuffer::Ptr data_;
      size_t size_;
  public:
      Buffer(RefBuffer::Ptr data, size_t size)
          : data_(data)
          , size_(size) {}

      char* data() const {
        return data_->data();
      }

      size_t size() const {
        return size_;
      }

      RefBuffer::Ptr get_buffer() const {
        return data_;
      }
  };

  virtual Buffer decompress(const Buffer& compressed_data) const = 0;
  virtual const char* get_method_name() const = 0;
  virtual ~ICompressor() {};
};

SharedRefPtr<ICompressor> get_compressor(const std::list<std::string>& methods,
        CassCqlCompression user_preference);

} // namespace cass

#endif

