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

#include "startup_request.hpp"
#include "serialization.hpp"

namespace cass {

bool StartupRequest::prepare(size_t reserved, char** output, size_t& size) {
  size = reserved + sizeof(int16_t);

  std::map<std::string, std::string> options;
  if (!compression_.empty()) {
    const char* key = "COMPRESSION";
    size += (sizeof(int16_t) + strlen(key));
    size += (sizeof(int16_t) + compression_.size());
    options[key] = compression_;
  }

  if (!version_.empty()) {
    const char* key = "CQL_VERSION";
    size += (sizeof(int16_t) + strlen(key));
    size += (sizeof(int16_t) + version_.size());
    options[key] = version_;
  }

  *output = new char[size];
  encode_string_map(*output + reserved, options);
  return true;
}

} // namespace cass
