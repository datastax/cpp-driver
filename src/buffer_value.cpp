#include "buffer_value.hpp"
#include "buffer_collection.hpp"

namespace cass {

BufferValue::BufferValue(const BufferCollection* collection)
  : size_(IS_COLLECTION) {
  collection->retain();
  data_.ref.collection = collection;
}

BufferValue::~BufferValue() {
  if (size_ > FIXED_BUFFER_SIZE) {
    data_.ref.array->release();
  } else if(size_ == IS_COLLECTION) {
    data_.ref.collection->release();
  }
}

const BufferCollection* BufferValue::collection() const {
  assert(is_collection());
  return static_cast<const BufferCollection*>(data_.ref.collection);
}

void BufferValue::copy(const BufferValue& buffer) {
  BufferRef temp = data_.ref;

  if (buffer.size_ > FIXED_BUFFER_SIZE) {
    buffer.data_.ref.array->retain();
    data_.ref.array = buffer.data_.ref.array;
  } else if(buffer.size_ == IS_COLLECTION) {
    buffer.data_.ref.collection->retain();
    data_.ref.collection = buffer.data_.ref.collection;
  } else if(buffer.size_ > 0) {
    memcpy(data_.fixed, buffer.data_.fixed, buffer.size_);
  }

  if (size_ > FIXED_BUFFER_SIZE) {
    temp.array->release();
  } else if(size_ == IS_COLLECTION) {
    temp.collection->release();
  }

  size_ = buffer.size_;
}

}
