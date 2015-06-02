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

#include "user_type_iterator.hpp"

#include "serialization.hpp"

namespace cass {

bool UserTypeIterator::next() {
  if (next_ == end_) {
    return false;
  }
  current_ = next_++;
  position_ = decode_field(position_);
  return true;
}

char* UserTypeIterator::decode_field(char* position) {
  int32_t size;
  char* buffer = decode_int32(position, size);
  value_ = Value(user_type_value_->protocol_version(), current_->type, buffer, size);
  return buffer + size;
}

} // namespace cass
