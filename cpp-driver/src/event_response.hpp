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
    NEW_NODE = 1,
    REMOVED_NODE,
    MOVED_NODE
  };

  enum StatusChange {
    UP = 1,
    DOWN
  };

  enum SchemaChange {
    CREATED = 1,
    UPDATED,
    DROPPED
  };

  enum SchemaChangeTarget {
    KEYSPACE = 1,
    TABLE,
    TYPE,
    FUNCTION,
    AGGREGATE
  };

  EventResponse()
      : Response(CQL_OPCODE_EVENT)
      , event_type_(0)
      , topology_change_(0)
      , status_change_(0)
      , schema_change_(0)
      , schema_change_target_(0) { }

  bool decode(int version, char* buffer, size_t size);

  int event_type() const { return event_type_; }
  int topology_change() const { return topology_change_; }
  int status_change() const { return status_change_; }
  const Address& affected_node() const { return affected_node_; }
  int schema_change() const { return schema_change_; }
  int schema_change_target() const { return schema_change_target_; }
  StringRef keyspace() const { return keyspace_; }
  StringRef target() const { return target_; }
  const StringRefVec& arg_types() const { return arg_types_; }

private:
  int event_type_;

  int topology_change_;
  int status_change_;
  Address affected_node_;

  int schema_change_;
  int schema_change_target_;
  StringRef keyspace_;
  StringRef target_;
  StringRefVec arg_types_;
};

} // namespace cass

#endif
