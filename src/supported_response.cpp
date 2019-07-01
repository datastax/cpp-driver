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

using namespace datastax::internal::core;

bool SupportedResponse::decode(Decoder& decoder) {
  decoder.set_type("supported");
  StringMultimap supported;

  CHECK_RESULT(decoder.decode_string_multimap(supported));
  decoder.maybe_log_remaining();

  StringMultimap::const_iterator it = supported.find("COMPRESSION");
  if (it != supported.end()) {
    compression_ = it->second;
  }

  it = supported.find("CQL_VERSIONS");
  if (it != supported.end()) {
    cql_versions_ = it->second;
  }

  it = supported.find("PROTOCOL_VERSIONS");
  if (it != supported.end()) {
    protocol_versions_ = it->second;
  }
  return true;
}
