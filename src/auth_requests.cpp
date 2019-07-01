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

#include "auth_requests.hpp"

using namespace datastax::internal::core;

int AuthResponseRequest::encode(ProtocolVersion version, RequestCallback* callback,
                                BufferVec* bufs) const {
  // <token> [bytes]
  size_t length = sizeof(int32_t) + token_.size();

  Buffer buf(length);
  buf.encode_long_string(0, token_.data(), token_.size());
  bufs->push_back(buf);

  return length;
}
