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

#include "abstract_data.hpp"

#include "collection.hpp"

namespace cass {

CassError AbstractData::set(size_t index, const Collection* value) {
  CASS_CHECK_INDEX_AND_TYPE(index, value);
  if (value->type() == CASS_COLLECTION_TYPE_MAP &&
      value->items().size() % 2 != 0) {
    return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
  }
  buffers_[index] = value->encode_with_length();
  return CASS_OK;
}

CassError AbstractData::set(size_t index, CassCustom custom) {
  CASS_CHECK_INDEX_AND_TYPE(index, custom);
  Buffer buf(custom.output_size);
  buffers_[index] = buf;
  *(custom.output) = reinterpret_cast<uint8_t*>(buf.data());
  return CASS_OK;
}

Buffer AbstractData::encode() const {
  Buffer buf(get_buffers_size());
  encode_buffers(0, &buf);
  return buf;
}

Buffer AbstractData::encode_with_length() const {
  size_t buffers_size = get_buffers_size();

  Buffer buf(sizeof(int32_t) + buffers_size);

  size_t pos = buf.encode_int32(0, buffers_size);
  encode_buffers(pos, &buf);

  return buf;
}

int32_t AbstractData::copy_buffers(BufferVec* bufs) const {
  int32_t size = 0;
  for (BufferVec::const_iterator i = buffers_.begin(),
       end = buffers_.end(); i != end; ++i) {
    if (i->size() > 0) {
      bufs->push_back(*i);
    } else  {
      bufs->push_back(cass::encode_with_length(CassNull()));
    }
    size += bufs->back().size();
  }
  return size;
}

size_t AbstractData::get_buffers_size() const {
  size_t size = 0;
  for (BufferVec::const_iterator i = buffers_.begin(),
       end = buffers_.end(); i != end; ++i) {
    if (i->size() > 0) {
      size += i->size();
    } else {
      size += sizeof(int32_t); // null
    }
  }
  return size;
}

void AbstractData::encode_buffers(size_t pos, Buffer* buf) const {
  for (BufferVec::const_iterator i = buffers_.begin(),
       end = buffers_.end(); i != end; ++i) {
    if (i->size() > 0) {
      pos = buf->copy(pos, i->data(), i->size());
    } else {
      pos = buf->encode_int32(pos, -1); // null
    }
  }
}

} // namespace cass
