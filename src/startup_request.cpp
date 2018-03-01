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

#include "serialization.hpp"

namespace cass {

int StartupRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  // <options> [string map]
  size_t length = sizeof(uint16_t);

  std::map<std::string, std::string> options;
  if (!compression_.empty()) {
    const char* key = "COMPRESSION";
    length += sizeof(uint16_t) + strlen(key);
    length += sizeof(uint16_t) + compression_.size();
    options[key] = compression_;
  }

  if (!version_.empty()) {
    const char* key = "CQL_VERSION";
    length += 2 + strlen(key);
    length += 2 + version_.size();
    options[key] = version_;
  }

  if (no_compact_enabled_) {
    const char* key = "NO_COMPACT";
    const char* value = "true";
    length += 2 + strlen(key);
    length += 2 + strlen(value);
    options[key] = value;
  }

  bufs->push_back(Buffer(length));
  bufs->back().encode_string_map(0, options);

  return length;
}

} // namespace cass
