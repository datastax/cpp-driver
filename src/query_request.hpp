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

#ifndef __CASS_QUERY_REQUEST_HPP_INCLUDED__
#define __CASS_QUERY_REQUEST_HPP_INCLUDED__

#include "statement.hpp"
#include "constants.hpp"

#include <string>
#include <vector>

namespace cass {

class QueryRequest : public Statement {
public:
  QueryRequest()
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY) {}

  QueryRequest(size_t value_count)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY, value_count) {}

  QueryRequest(const char* query, size_t query_length,
               size_t value_count)
      : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY, value_count) {
    set_query(query, query_length);
  }

private:
  ssize_t encode(int version, BufferValueVec* bufs) const;
  ssize_t encode_v1(BufferValueVec* bufs) const;
  ssize_t encode_v2(BufferValueVec* bufs) const;
};

} // namespace cass

#endif
