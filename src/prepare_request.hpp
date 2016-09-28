/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_PREPARE_REQUEST_HPP_INCLUDED__
#define __CASS_PREPARE_REQUEST_HPP_INCLUDED__

#include "request.hpp"
#include "constants.hpp"

#include <string>

namespace cass {

class PrepareRequest : public Request {
public:
  PrepareRequest(const std::string& query)
      : Request(CQL_OPCODE_PREPARE)
      , query_(query) { }

  const std::string& query() const { return query_; }


  void set_query(const std::string& query) { query_ = query; }

  void set_query(const char* query, size_t query_length) {
    query_.assign(query, query_length);
  }

private:
  int encode(int version, RequestCallback* callback, BufferVec* bufs) const;

private:
  std::string query_;
};

} // namespace cass

#endif
