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

#ifndef __CASS_RESULT_ITERATOR_HPP_INCLUDED__
#define __CASS_RESULT_ITERATOR_HPP_INCLUDED__

#include "iterator.hpp"
#include "result_response.hpp"
#include "row.hpp"

namespace cass {

class ResultIterator : public Iterator {
public:
  ResultIterator(const ResultResponse* result)
      : Iterator(CASS_ITERATOR_TYPE_RESULT)
      , result_(result)
      , index_(-1)
      , row_(result) {
    decoder_ = (const_cast<ResultResponse*>(result))->row_decoder();
    row_.values.reserve(result->column_count());
  }

  virtual bool next() {
    if (index_ + 1 >= result_->row_count()) {
      return false;
    }

    ++index_;

    if (index_ > 0) {
      return decode_row(decoder_, result_, row_.values);
    }

    return true;
  }

  const Row* row() const {
    assert(index_ >= 0 && index_ < result_->row_count());
    if (index_ > 0) {
      return &row_;
    } else {
      return &result_->first_row();
    }
  }

private:
  const ResultResponse* result_;
  Decoder decoder_;
  int32_t index_;
  Row row_;
};

} // namespace cass

#endif
