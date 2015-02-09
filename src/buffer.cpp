/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

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
    data_.ref.buffer->dec_ref();
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
    buffer.data_.ref.buffer->inc_ref();
    data_.ref.buffer = buffer.data_.ref.buffer;
  } else if (buffer.size_ == IS_COLLECTION) {
    buffer.data_.ref.collection->inc_ref();
    data_.ref.collection = buffer.data_.ref.collection;
  } else if (buffer.size_ > 0) {
    memcpy(data_.fixed, buffer.data_.fixed, buffer.size_);
  }

  if (size_ > FIXED_BUFFER_SIZE) {
    temp.buffer->dec_ref();
  } else if (size_ == IS_COLLECTION) {
    temp.collection->dec_ref();
  }

  size_ = buffer.size_;
}

}
