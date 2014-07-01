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

#include "prepare_request.hpp"
#include "serialization.hpp"

namespace cass {

int32_t PrepareRequestMessage::encode(int version, Writer::Bufs* bufs) {
  assert(version == 4);
  const PrepareRequest* prepare = static_cast<const PrepareRequest*>(request());
  int32_t length = 4 +  prepare->query().size();
  body_head_buf_ = Buffer(length);
  encode_long_string(body_head_buf_.data(), prepare->query().data(), prepare->query().size());
  bufs->push_back(uv_buf_init(body_head_buf_.data(), body_head_buf_.size()));
  return length;
}

} // namespace cas
