/*
  Copyright (c) DataStax, Inc.

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
#include "constants.hpp"
#include "request.hpp"
#include "tuple.hpp"
#include "user_type_value.hpp"

using namespace datastax::internal::core;

CassError AbstractData::set(size_t index, CassNull value) {
  CASS_CHECK_INDEX_AND_TYPE(index, value);
  elements_[index] = Element(value);
  return CASS_OK;
}

CassError AbstractData::set(size_t index, const Collection* value) {
  CASS_CHECK_INDEX_AND_TYPE(index, value);
  if (value->type() == CASS_COLLECTION_TYPE_MAP && value->items().size() % 2 != 0) {
    return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
  }
  elements_[index] = value;
  return CASS_OK;
}

CassError AbstractData::set(size_t index, const Tuple* value) {
  CASS_CHECK_INDEX_AND_TYPE(index, value);
  elements_[index] = value->encode_with_length();
  return CASS_OK;
}

CassError AbstractData::set(size_t index, const UserTypeValue* value) {
  CASS_CHECK_INDEX_AND_TYPE(index, value);
  elements_[index] = value->encode_with_length();
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

size_t AbstractData::get_buffers_size() const {
  size_t size = 0;
  for (ElementVec::const_iterator i = elements_.begin(), end = elements_.end(); i != end; ++i) {
    if (!i->is_unset()) {
      size += i->get_size();
    } else {
      size += sizeof(int32_t); // null
    }
  }
  return size;
}

void AbstractData::encode_buffers(size_t pos, Buffer* buf) const {
  for (ElementVec::const_iterator i = elements_.begin(), end = elements_.end(); i != end; ++i) {
    if (!i->is_unset()) {
      pos = i->copy_buffer(pos, buf);
    } else {
      pos = buf->encode_int32(pos, -1); // null
    }
  }
}

size_t AbstractData::Element::get_size() const {
  if (type_ == COLLECTION) {
    return collection_->get_size_with_length();
  } else {
    assert(type_ == BUFFER || type_ == NUL);
    return buf_.size();
  }
}

size_t AbstractData::Element::copy_buffer(size_t pos, Buffer* buf) const {
  if (type_ == COLLECTION) {
    Buffer encoded(collection_->encode_with_length());
    return buf->copy(pos, encoded.data(), encoded.size());
  } else {
    assert(type_ == BUFFER || type_ == NUL);
    return buf->copy(pos, buf_.data(), buf_.size());
  }
}

Buffer AbstractData::Element::get_buffer() const {
  if (type_ == COLLECTION) {
    return collection_->encode_with_length();
  } else {
    assert(type_ == BUFFER || type_ == NUL);
    return buf_;
  }
}
