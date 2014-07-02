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

#include "request.hpp"
#include "serialization.hpp"
#include "writer.hpp"
#include "batch_request.hpp"
#include "query_request.hpp"
#include "execute_request.hpp"
#include "prepare_request.hpp"
#include "options_request.hpp"
#include "startup_request.hpp"

#define CASS_HEADER_SIZE_V1_AND_V2 8
#define CASS_HEADER_SIZE_V3 9

namespace cass {

BufferVec* Request::encode(int version, int flags, int stream) const {
  ScopedPtr<BufferVec> bufs(new BufferVec());

  if(version == 1 || version == 2) {
    bufs->push_back(Buffer()); // Placeholder

    int32_t length = encode(version, bufs.get());
    if(length < 0) {
      return NULL;
    }

    Buffer buf(CASS_HEADER_SIZE_V1_AND_V2);
    size_t pos = 0;
    pos = buf.encode_byte(pos, version);
    pos = buf.encode_byte(pos, flags);
    pos = buf.encode_byte(pos, stream);
    pos = buf.encode_byte(pos, opcode());
    buf.encode_int32(pos, length);
    (*bufs)[0] = buf;
  } else {
    return NULL;
  }

  return bufs.release();
}

} // namespace cass
