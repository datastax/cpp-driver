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

#ifndef CQL_VECTOR_STREAM_H_
#define CQL_VECTOR_STREAM_H_

#include <streambuf>
#include <vector>
#include "cql/cql.hpp"

namespace cql {

struct vector_stream_t : std::streambuf {
    vector_stream_t(std::vector<cql::cql_byte_t>& vec) {
        char* start = reinterpret_cast<char*>(&vec[0]);
        this->setg(start, start, start + vec.size());
        this->setp(start, start + vec.size());
    }

    vector_stream_t(std::vector<cql::cql_byte_t>& vec,
                    size_t offset) {
        char* start = reinterpret_cast<char*>(&vec[0]);
        this->setg(start, start + offset, start + vec.size());
        this->setp(start + offset, start + vec.size());

    }

    vector_stream_t(std::vector<cql::cql_byte_t>& vec,
                    size_t offset,
                    size_t limit) {
        char* start = reinterpret_cast<char*>(&vec[0]);
        this->setg(start, start + offset, start + offset + limit);
        this->setp(start + offset, start + offset + limit);
    }
};
} // namespace cql

#endif // CQL_VECTOR_STREAM_H_
