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

#include "map_iterator.hpp"

namespace cass {

char* MapIterator::decode_pair(char* position) {
  int protocol_version = map_->protocol_version();

  int32_t size;

  position = decode_size(protocol_version, position, size);
  key_ = Value(protocol_version, map_->primary_data_type(), position, size);

  position = decode_size(protocol_version, position + size, size);
  value_ = Value(protocol_version, map_->secondary_data_type(), position, size);

  return position + size;
}

bool MapIterator::next() {
  if (index_ + 1 >= count_) {
    return false;
  }
  ++index_;
  position_ = decode_pair(position_);
  return true;
}

} // namespace cass
