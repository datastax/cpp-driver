/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#ifndef CQL_FUTURE_RESULT_H_
#define CQL_FUTURE_RESULT_H_

#include "cql/cql.hpp"
#include "cql/cql_error.hpp"
#include "cql/cql_result.hpp"
#include "cql/cql_stream.hpp"

namespace cql {

// Forward declarations
class cql_connection_t;

struct cql_future_result_t {

    cql_future_result_t() :
        stream()
    {}

    cql_future_result_t(
        boost::shared_ptr<cql::cql_connection_t> client_,
        const cql::cql_stream_t&                 stream_,
        boost::shared_ptr<cql_result_t>          result_) :
            client(client_),
            stream(stream_),
            result(result_)
    {}

    cql_future_result_t(
        boost::shared_ptr<cql::cql_connection_t> client_,
        const cql::cql_stream_t&                 stream_,
        cql::cql_error_t                         error_) :
            client(client_),
            stream(stream_),
            error(error_)
    {}

    boost::shared_ptr<cql::cql_connection_t> client;
    cql::cql_stream_t                        stream;
    boost::shared_ptr<cql::cql_result_t>     result;
    cql::cql_error_t                         error;
};


} // namespace cql

#endif // CQL_FUTURE_RESULT_H_
