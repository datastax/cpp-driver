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

#ifndef __CASS_EVENT_RESPONSE_HPP_INCLUDED__
#define __CASS_EVENT_RESPONSE_HPP_INCLUDED__

#include "address.hpp"
#include "response.hpp"
#include "constants.hpp"
#include "scoped_ptr.hpp"
#include "string_ref.hpp"

namespace cass {

class EventResponse : public Response {
public:
  enum TopologyChange {
    NEW_NODE,
    REMOVED_NODE,
    MOVED_NODE
  };

  enum StatusChange {
    UP,
    DOWN
  };

  enum SchemaChange {
    CREATED,
    UPDATED,
    DROPPED
  };

  EventResponse()
      : Response(CQL_OPCODE_EVENT)
      , event_type_(0)
      , keyspace_(NULL)
      , keyspace_size_(0)
      , table_(NULL)
      , table_size_(0) {}

  bool decode(int version, char* buffer, size_t size);

  int event_type() const { return event_type_; }

  TopologyChange topology_change() const {
    return topology_change_;
  }

  StatusChange status_change() const {
    return status_change_;
  }

  const Address& affected_node() const {
    return affected_node_;
  }

  SchemaChange schema_change() const {
    return schema_change_;
  }

  StringRef keyspace() const {
    return StringRef(keyspace_, keyspace_size_);
  }

  StringRef table() const {
    return StringRef(table_, table_size_);
  }

private:
  int event_type_;

  TopologyChange topology_change_;
  StatusChange status_change_;
  Address affected_node_;

  SchemaChange schema_change_;
  char* keyspace_;
  size_t keyspace_size_;
  char* table_;
  size_t table_size_;

};

} // namespace cass

#endif
