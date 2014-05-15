/*
  Copyright 2014 DataStax

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
#include "body_result.hpp"
#include "buffer_piece.hpp"

namespace cass {

typedef std::vector<BufferPiece> Row;

struct ResultIterator : Iterator {
  Result*             result;
  int32_t             row_position;
  char*               position;
  char*               position_next;
  Row                 row;

  ResultIterator(
      Result* result) :
      Iterator(CASS_ITERATOR_TYPE_RESULT),
      result(result),
      row_position(0),
      position(result->rows),
      row(result->column_count) {
    position_next = parse_row(position, row);
  }

  char*
  parse_row(
      char* row,
      std::vector<BufferPiece>& output) {
    char* buffer = row;
    output.clear();

    for (int i = 0; i < result->column_count; ++i) {
      int32_t size  = 0;
      buffer        = decode_int(buffer, size);
      output.push_back(BufferPiece(buffer, size));
      buffer       += size;
    }
    return buffer;
  }

  virtual bool
  next() {
    ++row_position;
    if (row_position >= result->row_count) {
      return false;
    }
    position_next = parse_row(position_next, row);
    return true;
  }
};

} // namespace cass

#endif
