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

#ifndef DATASTAX_INTERNAL_ROW_ITERATOR_HPP
#define DATASTAX_INTERNAL_ROW_ITERATOR_HPP

#include "iterator.hpp"
#include "row.hpp"

namespace datastax { namespace internal { namespace core {

class RowIterator : public Iterator {
public:
  RowIterator(const Row* row)
      : Iterator(CASS_ITERATOR_TYPE_ROW)
      , row_(row)
      , index_(-1) {}

  virtual bool next() {
    if (static_cast<size_t>(index_ + 1) >= row_->values.size()) {
      return false;
    }
    ++index_;
    return true;
  }

  const Value* column() const {
    assert(index_ >= 0 && static_cast<size_t>(index_) < row_->values.size());
    return &row_->values[index_];
  }

private:
  const Row* row_;
  int32_t index_;
};

}}} // namespace datastax::internal::core

#endif
