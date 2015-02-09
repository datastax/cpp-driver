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

#ifndef __CASS_QUERY_REQUEST_HPP_INCLUDED__
#define __CASS_QUERY_REQUEST_HPP_INCLUDED__

#include "statement.hpp"
#include "constants.hpp"

#include <string>
#include <vector>

namespace cass {

class QueryRequest : public Statement {
public:
  explicit QueryRequest(size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY, value_count) {}


  QueryRequest(const std::string& query, size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY, value_count)
    , query_(query) {}

  QueryRequest(const char* query, size_t query_length,
               size_t value_count)
      : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY, value_count)
      , query_(query, query_length) {}

  const std::string& query() const { return query_; }

  void set_query(const std::string& query) {
    query_ = query;
  }

  void set_query(const char* query, size_t query_length) {
    query_.assign(query, query_length);
  }

private:
  int encode(int version, BufferVec* bufs) const;
  int encode_v1(BufferVec* bufs) const;
  int encode_v2(BufferVec* bufs) const;

private:
  std::string query_;
};

} // namespace cass

#endif
