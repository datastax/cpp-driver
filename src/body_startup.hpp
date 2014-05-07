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

#ifndef __CASS_BODY_STARTUP_HPP_INCLUDED__
#define __CASS_BODY_STARTUP_HPP_INCLUDED__

#include <map>
#include <string>

#include "message_body.hpp"

namespace cass {

struct BodyStartup
    : public MessageBody {
  std::unique_ptr<char> guard;
  std::string           version;
  std::string           compression;

  BodyStartup() :
      version("3.0.0"),
      compression("")
  {}

  uint8_t
  opcode() const {
    return CASS_OPCODE_STARTUP;
  }

  bool
  consume(
      char*  buffer,
      size_t size) {
    (void) size;

    OptionsCollection options;
    decode_string_map(buffer, options);
    OptionsCollection::const_iterator it = options.find("COMPRESSION");
    if (it != options.end()) {
      compression = it->second;
    }

    it = options.find("CASS_VERSION");
    if (it != options.end()) {
      version = it->second;
    }
    return true;
  }

  bool
  prepare(
      size_t  reserved,
      char**  output,
      size_t& size) {
    size = reserved + sizeof(int16_t);

    std::map<std::string, std::string> options;
    if (!compression.empty()) {
      const char* key = "COMPRESSION";
      size += (sizeof(int16_t) + strlen(key));
      size += (sizeof(int16_t) + compression.size());
      options[key] = compression;
    }

    if (!version.empty()) {
      const char* key = "CASS_VERSION";
      size += (sizeof(int16_t) + strlen(key));
      size += (sizeof(int16_t) + version.size());
      options[key] = version;
    }

    *output = new char[size];
    encode_string_map(*output + reserved, options);
    return true;
  }

 private:
  typedef std::map<std::string, std::string> OptionsCollection;

  BodyStartup(const BodyStartup&) {}
  void operator=(const BodyStartup&) {}
};

} // namespace cass

#endif
