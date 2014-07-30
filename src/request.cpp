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

#include "constants.hpp"
#include "serialization.hpp"

namespace cass {

bool Request::encode( int version, int flags, int stream, BufferVec* bufs) const {
  bufs->clear();

  if (version == 1 || version == 2) {
    bufs->push_back(Buffer()); // Placeholder

    int32_t length = encode(version, bufs);
    if (length < 0) {
      return false;
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
    return false;
  }

  return true;
}

} // namespace cass
