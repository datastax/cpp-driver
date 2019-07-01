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

#ifndef DATASTAX_INTERNAL_PREPARE_REQUEST_HPP
#define DATASTAX_INTERNAL_PREPARE_REQUEST_HPP

#include "constants.hpp"
#include "request.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class PrepareRequest : public Request {
public:
  typedef SharedRefPtr<PrepareRequest> Ptr;
  typedef SharedRefPtr<const PrepareRequest> ConstPtr;

  PrepareRequest(const String& query)
      : Request(CQL_OPCODE_PREPARE)
      , query_(query) {}

  const String& query() const { return query_; }

  void set_query(const String& query) { query_ = query; }

  void set_query(const char* query, size_t query_length) { query_.assign(query, query_length); }

private:
  int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

private:
  String query_;
};

}}} // namespace datastax::internal::core

#endif
