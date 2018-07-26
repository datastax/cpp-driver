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

#include "register_request.hpp"

#include "serialization.hpp"

namespace cass {

int RegisterRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  // <events> [string list]
  size_t length = sizeof(uint16_t);
  Vector<String> events;

  if (event_types_ & CASS_EVENT_TOPOLOGY_CHANGE) {
    events.push_back("TOPOLOGY_CHANGE");
    length += sizeof(uint16_t);
    length += events.back().size();
  }

  if (event_types_ & CASS_EVENT_STATUS_CHANGE) {
    events.push_back("STATUS_CHANGE");
    length += sizeof(uint16_t);
    length += events.back().size();
  }

  if (event_types_ & CASS_EVENT_SCHEMA_CHANGE) {
    events.push_back("SCHEMA_CHANGE");
    length += sizeof(uint16_t);
    length += events.back().size();
  }

  bufs->push_back(Buffer(length));
  bufs->back().encode_string_list(0, events);

  return length;
}

} // namespace cas

