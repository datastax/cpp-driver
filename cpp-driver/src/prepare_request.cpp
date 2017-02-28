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

#include "prepare_request.hpp"

#include "serialization.hpp"

namespace cass {

int PrepareRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  // <query> [long string]
  size_t length = sizeof(int32_t) +  query_.size();
  bufs->push_back(Buffer(length));
  bufs->back().encode_long_string(0, query_.data(), query().size());
  return length;
}

} // namespace cass
