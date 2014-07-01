#include "buffer.hpp"
#include "collection.hpp"

namespace cass {

class RefCollection : public Ref {
public:
  RefCollection(const Collection* collection)
    : collection_(collection) { }

  const Collection* collection() const { return collection_.get(); }

private:
  ScopedRefPtr<const Collection> collection_;
};

Buffer::Buffer(const char* data, int32_t size)
  : size_(size) {
  if (size > FIXED_BUFFER_SIZE) {
    RefArray* array = new RefArray(size);
    memcpy(array->data(), data, size);
    data_.ref = array;
  } else if (size > 0) {
    memcpy(data_.fixed, data, size);
  }
}

Buffer::Buffer(int32_t size)
  : size_(size) {
  if (size > FIXED_BUFFER_SIZE) {
    data_.ref = new RefArray(size);
  }
}

Buffer::Buffer(const Collection* collection)
  : size_(IS_COLLECTION) {
  data_.ref = new RefCollection(collection);
}

Buffer::Buffer(const Buffer& Buffer) {
  copy(Buffer);
}

Buffer::~Buffer() {
  if (size_ > FIXED_BUFFER_SIZE || size_ == IS_COLLECTION) {
    data_.ref->release();
  }
}

const Collection* Buffer::collection() const {
  assert(is_collection());
  return static_cast<RefCollection*>(data_.ref)->collection();
}

void Buffer::copy(const Buffer& buffer) {
  size_ = buffer.size_;
  if (size_ > 0) {
    if (size_ > FIXED_BUFFER_SIZE || size_ == IS_COLLECTION) {
      buffer.data_.ref->retain();
      Ref* temp = data_.ref;
      data_.ref = buffer.data_.ref;
      temp->release();
    } else {
      memcpy(data_.fixed, buffer.data_.fixed, size_);
    }
  }
}

}
