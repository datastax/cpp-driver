#include "buffer.hpp"
#include "buffer_collection.hpp"

namespace cass {

Buffer::Buffer(const BufferCollection* collection)
  : size_(IS_COLLECTION) {
  collection->inc_ref();
  data_.ref.collection = collection;
}

Buffer::~Buffer() {
  if (size_ > FIXED_BUFFER_SIZE) {
    data_.ref.array->dec_ref();
  } else if (size_ == IS_COLLECTION) {
    data_.ref.collection->dec_ref();
  }
}

const BufferCollection* Buffer::collection() const {
  assert(is_collection());
  return static_cast<const BufferCollection*>(data_.ref.collection);
}

void Buffer::copy(const Buffer& buffer) {
  BufferRef temp = data_.ref;

  if (buffer.size_ > FIXED_BUFFER_SIZE) {
    buffer.data_.ref.array->inc_ref();
    data_.ref.array = buffer.data_.ref.array;
  } else if (buffer.size_ == IS_COLLECTION) {
    buffer.data_.ref.collection->inc_ref();
    data_.ref.collection = buffer.data_.ref.collection;
  } else if (buffer.size_ > 0) {
    memcpy(data_.fixed, buffer.data_.fixed, buffer.size_);
  }

  if (size_ > FIXED_BUFFER_SIZE) {
    temp.array->dec_ref();
  } else if (size_ == IS_COLLECTION) {
    temp.collection->dec_ref();
  }

  size_ = buffer.size_;
}

}
