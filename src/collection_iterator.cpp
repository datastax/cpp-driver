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

#include "collection_iterator.hpp"

namespace cass {

char* CollectionIterator::decode_value(char* position) {
  uint16_t size;
  char* buffer = decode_uint16(position, size);

  CassValueType type;
  if (collection_->type() == CASS_VALUE_TYPE_MAP) {
    type = (index_ % 2 == 0) ? collection_->primary_type()
                             : collection_->secondary_type();
  } else {
    type = collection_->primary_type();
  }

  value_ = Value(type, buffer, size);

  return buffer + size;
}

bool CollectionIterator::next() {
  if (index_ + 1 >= count_) {
    return false;
  }
  ++index_;
  position_ = decode_value(position_);
  return true;
}

} // namespace cass
