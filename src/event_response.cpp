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

#include "event_repsonse.hpp"

#include "serialization.hpp"

namespace cass {

char* decode_string_ref(char* buffer, boost::string_ref* output) {
  char* str;
  size_t str_size;
  buffer = decode_string(buffer, &str, str_size);
  *output = boost::string_ref(str, str_size);
  return buffer;
}

bool EventResponse::decode(int version, char* buffer, size_t size) {
  boost::string_ref event_type;
  buffer = decode_string_ref(buffer, &event_type);

  if (event_type == "TOPOLOGY_CHANGE") {
    boost::string_ref topology_change;
    buffer = decode_string_ref(buffer, &topology_change);
    if (topology_change == "NEW_NODE") {
      topology_change_ = NEW_NODE;
    } else if(topology_change == "REMOVE_NODE") {
      topology_change_ = REMOVE_NODE;
    } else {
      return false;
    }
    // Get the inet
  } else if(event_type == "STATUS_CHANGE") {
    boost::string_ref status_change;
    buffer = decode_string_ref(buffer, &status_change);
    if (status_change == "UP") {
      status_change_ = UP;
    } else if(status_change == "DOWN") {
      status_change_ = DOWN;
    } else {
      return false;
    }
    // Get the inet
  } else if(event_type == "SCHEMA_CHANGE") {
    boost::string_ref schema_change;
    buffer = decode_string_ref(buffer, &schema_change);
    if (schema_change == "CREATED") {
      schema_change_ = CREATED;
    } else if(schema_change == "UPDATED") {
      schema_change_ = UPDATED;
    } else if(schema_change == "DROPPED") {
      schema_change_ = DROPPED;
    } else {
      return false;
    }
    buffer = decode_string(buffer, &keyspace_, keyspace_size_);
    buffer = decode_string(buffer, &table_, table_size_);
  } else {
    return false;
  }

  return true;
}

} // namespace cass
