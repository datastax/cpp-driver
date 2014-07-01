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

int32_t StartupRequestMessage::encode(int version, Writer::Bufs* bufs) {
  assert(version == 2);

  int32_t length = 2;
  std::map<std::string, std::string> options;

  const StartupRequest* startup = static_cast<const StartupRequest*>(request());

  if (!startup->compression().empty()) {
    const char* key = "COMPRESSION";
    length += 2 + strlen(key);
    length += 2 + startup->compression().size();
  }

  if(!startup->version().empty()) {
    const char* key = "CQL_VERSION";
    length += 2 + strlen(key);
    length += 2 + startup->version().size();
  }

  body_head_buf_ = Buffer(length);
  encode_string_map(body_head_buf_.data(), options);
  bufs->push_back(uv_buf_init(body_head_buf_.data(), body_head_buf_.size()));

  return length;
}

} // namespace cass
