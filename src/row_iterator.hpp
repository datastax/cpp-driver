/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_ROW_ITERATOR_HPP_INCLUDED__
#define __CASS_ROW_ITERATOR_HPP_INCLUDED__

#include "iterator.hpp"
#include "row.hpp"

namespace cass {

class RowIterator : public Iterator {
public:
  RowIterator(const Row* row)
      : Iterator(CASS_ITERATOR_TYPE_ROW)
      , row_(row)
      , index_(0) {}

  virtual bool next() {
    if (index_++ < row_->size()) {
      return true;
    }
    return false;
  }

  const Value* column() { return &(*row_)[index_]; }

private:
  const Row* row_;
  size_t index_;
};

} // namespace cass

#endif
