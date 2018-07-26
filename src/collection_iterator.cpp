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
  return decode_value();
}

bool CollectionIterator::decode_value() {
  DataType::ConstPtr data_type;
  if (collection_->value_type() == CASS_VALUE_TYPE_MAP) {
    data_type = (index_ % 2 == 0) ? collection_->primary_data_type()
                                  : collection_->secondary_data_type();
  } else {
    data_type = collection_->primary_data_type();
  }

  return decoder_.decode_value(data_type, value_, true);
}

bool TupleIterator::next() {
  if (next_ == end_) {
    return false;
  }
  current_ = next_++;
  return decoder_.decode_value(*current_, value_);
}

} // namespace cass
