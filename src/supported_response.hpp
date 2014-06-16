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

#ifndef __CASS_SUPPORTED_RESPONSE_HPP_INCLUDED__
#define __CASS_SUPPORTED_RESPONSE_HPP_INCLUDED__

#include <list>
#include <string>

#include "message_body.hpp"

namespace cass {

struct SupportedResponse : public MessageBody {
  std::list<std::string> compression;
  std::list<std::string> versions;

  SupportedResponse()
      : MessageBody(CQL_OPCODE_SUPPORTED) {}

  bool consume(char* buffer, size_t size) {
    string_multimap_t supported;

    decode_string_multimap(buffer, supported);
    string_multimap_t::const_iterator it = supported.find("COMPRESSION");
    if (it != supported.end()) {
      compression = it->second;
    }

    it = supported.find("CASS_VERSION");
    if (it != supported.end()) {
      versions = it->second;
    }
    return true;
  }
};

} // namespace cass

#endif
