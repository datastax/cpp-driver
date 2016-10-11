/*
  Copyright (c) 2014-2016 DataStax

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

#include "auth_requests.hpp"

namespace cass {

int CredentialsRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  if (version != 1) {
    return -1;
  }

  // <n> [short] <pair_0> [string][string] ... <pair_n> [string][string]
  size_t length = sizeof(uint16_t);

  for (V1Authenticator::Credentials::const_iterator it = credentials_.begin(),
       end = credentials_.end(); it != end; ++it) {
    length += sizeof(uint16_t) + it->first.size();
    length += sizeof(uint16_t) + it->second.size();
  }

  Buffer buf(length);
  buf.encode_string_map(0, credentials_);
  bufs->push_back(buf);

  return length;
}

int AuthResponseRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  if (version < 2) {
    return -1;
  }

  // <token> [bytes]
  size_t length = sizeof(int32_t) + token_.size();

  Buffer buf(length);
  buf.encode_long_string(0, token_.data(), token_.size());
  bufs->push_back(buf);

  return length;
}

} // namespace cass
