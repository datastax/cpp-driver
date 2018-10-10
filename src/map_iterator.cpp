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

bool MapIterator::decode_pair() {
  if (!decoder_.decode_value(map_->primary_data_type(), key_, true)) return false;
  return decoder_.decode_value(map_->secondary_data_type(), value_, true);
}

bool MapIterator::next() {
  if (index_ + 1 >= count_) {
    return false;
  }
  ++index_;
  return decode_pair();
}

} // namespace cass
