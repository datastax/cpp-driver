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

#include "user_type_field_iterator.hpp"

#include "serialization.hpp"

using namespace datastax::internal::core;

bool UserTypeFieldIterator::next() {
  if (next_ == end_) {
    return false;
  }
  current_ = next_++;
  value_ = decoder_.decode_value(current_->type);
  return value_.is_valid();
}
