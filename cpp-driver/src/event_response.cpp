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

#include "event_response.hpp"

#include "serialization.hpp"

namespace cass {

bool EventResponse::decode(int version, char* buffer, size_t size) {
  StringRef event_type;
  char* pos = decode_string(buffer, &event_type);

  if (event_type == "TOPOLOGY_CHANGE") {
    event_type_ = CASS_EVENT_TOPOLOGY_CHANGE;

    StringRef topology_change;
    pos = decode_string(pos, &topology_change);
    if (topology_change == "NEW_NODE") {
      topology_change_ = NEW_NODE;
    } else if (topology_change == "REMOVED_NODE") {
      topology_change_ = REMOVED_NODE;
    } else if (topology_change == "MOVED_NODE") {
      topology_change_ = MOVED_NODE;
    } else {
      return false;
    }
    decode_inet(pos, &affected_node_);
  } else if (event_type == "STATUS_CHANGE") {
    event_type_ = CASS_EVENT_STATUS_CHANGE;

    StringRef status_change;
    pos = decode_string(pos, &status_change);
    if (status_change == "UP") {
      status_change_ = UP;
    } else if (status_change == "DOWN") {
      status_change_ = DOWN;
    } else {
      return false;
    }
    decode_inet(pos, &affected_node_);
  } else if (event_type == "SCHEMA_CHANGE") {
    event_type_ = CASS_EVENT_SCHEMA_CHANGE;

    // Version 1+: <change>... (all start with change type)
    StringRef schema_change;
    pos = decode_string(pos, &schema_change);
    if (schema_change == "CREATED") {
      schema_change_ = CREATED;
    } else if (schema_change == "UPDATED") {
      schema_change_ = UPDATED;
    } else if (schema_change == "DROPPED") {
      schema_change_ = DROPPED;
    } else {
      return false;
    }

    if (version <= 2) {
      // Version 1 and 2: ...<keyspace><table> ([string][string])
      pos = decode_string(pos, &keyspace_);
      decode_string(pos, &target_);
      schema_change_target_ = target_.size() == 0 ? KEYSPACE : TABLE;
    } else {
      // Version 3+: ...<target><options>
      // <target> = [string]
      // <options> = [string] OR [string][string]

      StringRef target;
      pos = decode_string(pos, &target);
      if (target == "KEYSPACE") {
        schema_change_target_ = KEYSPACE;
      } else if (target == "TABLE") {
        schema_change_target_ = TABLE;
      } else if (target == "TYPE") {
        schema_change_target_ = TYPE;
      } else if (target == "FUNCTION") {
        schema_change_target_ = FUNCTION;
      } else if (target == "AGGREGATE") {
        schema_change_target_ = AGGREGATE;
      } else {
        return false;
      }

      pos = decode_string(pos, &keyspace_);

      if (schema_change_target_ == TABLE ||
          schema_change_target_ == TYPE) {
        decode_string(pos, &target_);
      } else if (schema_change_target_ == FUNCTION ||
                 schema_change_target_ == AGGREGATE) {
        pos = decode_string(pos, &target_);
        decode_stringlist(pos, arg_types_);
      }
    }
  } else {
    return false;
  }

  return true;
}

} // namespace cass
