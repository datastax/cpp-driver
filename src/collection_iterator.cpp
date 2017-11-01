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

#include "collection_iterator.hpp"

namespace cass {

bool CollectionIterator::next() {
  if (index_ + 1 >= count_) {
    return false;
  }
  ++index_;
  position_ = decode_value(position_);
  return true;
}

char* CollectionIterator::decode_value(char* position) {
  int protocol_version = collection_->protocol_version();

  int32_t size;
  char* buffer = decode_size(protocol_version, position, size);

  DataType::ConstPtr data_type;
  if (collection_->value_type() == CASS_VALUE_TYPE_MAP) {
    data_type = (index_ % 2 == 0) ? collection_->primary_data_type()
                                  : collection_->secondary_data_type();
  } else {
    data_type = collection_->primary_data_type();
  }

  value_ = Value(protocol_version, data_type, buffer, size);

  return buffer + size;
}

bool TupleIterator::next() {
  if (next_ == end_) {
    return false;
  }
  current_ = next_++;
  position_ = decode_value(position_);
  return true;
}

char* TupleIterator::decode_value(char* position) {
  int32_t size;
  char* buffer = decode_int32(position, size);

  value_ = Value(tuple_->protocol_version(), *current_, buffer, size);

  return size > 0 ? buffer + size : buffer;
}

} // namespace cass
