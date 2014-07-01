#include "buffer.hpp"
#include "collection.hpp"

namespace cass {

class RefCollectionBuffer : public RefBuffer {
public:
  RefCollectionBuffer(const Collection* collection)
    : collection_(collection) { }

  int32_t encode(int version, Buffer* buffer) const {
    return collection_->encode(version, buffer);
  }

private:
  ScopedRefPtr<const Collection> collection_;
};

Buffer::Buffer(const char* data, int32_t size)
  : size_(size) {
  if (size > FIXED_BUFFER_SIZE) {
    data_.ref = new RefBuffer(size);
    memcpy(data_.ref->data(), data, size);
  } else if (size > 0) {
    memcpy(data_.fixed, data, size);
  }
}

Buffer::Buffer(int32_t size)
  : size_(size) {
  if (size > FIXED_BUFFER_SIZE) {
    data_.ref = new RefBuffer(size);
  }
}

Buffer::Buffer(const Collection* collection)
  : size_(-2) {
  data_.ref = new RefCollectionBuffer(collection);
}

int32_t Buffer::encode_collection(int version, Buffer* buffer) const {
  assert(is_collection());
  return static_cast<RefCollectionBuffer*>(data_.ref)->encode(version, buffer);
}

void Buffer::copy(const Buffer& buffer) {
  size_ = buffer.size_;
  if (size_ > 0) {
    if (size_ > FIXED_BUFFER_SIZE || size_ == -2) {
      buffer.data_.ref->retain();
      RefBuffer* temp = data_.ref;
      data_.ref = buffer.data_.ref;
      temp->release();
    } else {
      memcpy(data_.fixed, buffer.data_.fixed, size_);
    }
  }
}

}
