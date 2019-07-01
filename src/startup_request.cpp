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

#include "startup_request.hpp"
#include "driver_info.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

int StartupRequest::encode(ProtocolVersion version, RequestCallback* callback,
                           BufferVec* bufs) const {
  // <options> [string map]
  size_t length = sizeof(uint16_t);

  OptionsMap options;
  if (!application_name_.empty()) {
    options["APPLICATION_NAME"] = application_name_;
  }
  if (!application_version_.empty()) {
    options["APPLICATION_VERSION"] = application_version_;
  }
  if (!client_id_.empty()) {
    options["CLIENT_ID"] = client_id_;
  }
  options["CQL_VERSION"] = CASS_DEFAULT_CQL_VERSION;
  options["DRIVER_NAME"] = driver_name();
  options["DRIVER_VERSION"] = driver_version();
  if (no_compact_enabled_) {
    options["NO_COMPACT"] = "true";
  }

  for (OptionsMap::const_iterator it = options.begin(), end = options.end(); it != end; ++it) {
    length += sizeof(uint16_t) + it->first.size();
    length += sizeof(uint16_t) + it->second.size();
  }

  bufs->push_back(Buffer(length));
  bufs->back().encode_string_map(0, options);

  return length;
}
