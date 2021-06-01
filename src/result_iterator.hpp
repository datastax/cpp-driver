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

#ifndef DATASTAX_INTERNAL_RESULT_ITERATOR_HPP
#define DATASTAX_INTERNAL_RESULT_ITERATOR_HPP

#include "iterator.hpp"
#include "result_response.hpp"
#include "row.hpp"

namespace datastax { namespace internal { namespace core {

class ResultIterator : public Iterator {
public:
  ResultIterator(const ResultResponse* result)
      : Iterator(CASS_ITERATOR_TYPE_RESULT)
      , result_(result)
      , index_(-1)
      , row_(result) {
    decoder_ = (const_cast<ResultResponse*>(result))->row_decoder();
    row_.values = result_->first_row().values;
  }

  virtual bool next() {
    return (index_ + 1 < result_->row_count()) &&
           (++index_ < 1 || decode_next_row(decoder_, row_.values));
  }

  const Row* row() const {
    assert(index_ >= 0 && index_ < result_->row_count());
    return &row_;
  }

private:
  const ResultResponse* result_;
  Decoder decoder_;
  int32_t index_;
  Row row_;
};

}}} // namespace datastax::internal::core

#endif
