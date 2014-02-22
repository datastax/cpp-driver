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

#ifndef CQL_EVENT_H_
#define CQL_EVENT_H_

#include "cql/cql.hpp"

namespace cql {

class cql_event_t {
public:
    virtual cql_event_enum
    event_type() const = 0;

    virtual cql_event_topology_enum
    topology_change() const = 0;

    virtual cql_event_status_enum
    status_change() const = 0;

    virtual cql_event_schema_enum
    schema_change() const = 0;

    virtual const std::string&
    keyspace() const = 0;

    virtual const std::string&
    column_family() const = 0;

    virtual const std::string&
    ip() const = 0;

    virtual cql_int_t
    port() const = 0;

    virtual
    ~cql_event_t() {};
};

} // namespace cql

#endif // CQL_EVENT_H_
