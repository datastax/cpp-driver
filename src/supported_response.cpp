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

#include "supported_response.hpp"

#include "serialization.hpp"

namespace cass {

bool SupportedResponse::decode(int version, char* buffer, size_t size) {
  StringMultimap supported;

  decode_string_multimap(buffer, supported);
  StringMultimap::const_iterator it = supported.find("COMPRESSION");
  if (it != supported.end()) {
    compression_ = it->second;
  }

  it = supported.find("CASS_VERSION");
  if (it != supported.end()) {
    versions_ = it->second;
  }
  return true;
}

} // namespace cass
