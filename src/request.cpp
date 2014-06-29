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

#define CASS_HEADER_SIZE_V1_AND_V2 8
#define CASS_HEADER_SIZE_V3 9

namespace cass {

bool Request::encode(int version, int flags, int stream, BufferVec* bufs) {
  if(version == 1 || version == 2) {
    Buffer header_buf(CASS_HEADER_SIZE_V1_AND_V2);

    char* data = header_buf.data();

    data[0] = version;
    data[1] = flags;
    data[2] = stream;
    data[3] = opcode_;

    bufs->push_back(header_buf);

    int32_t length = encode(version, bufs);
    if(length < 0) {
      return false;
    }

    encode_int(data + 4, length);
  } else {
    return false;
  }

  return true;
}

} // namespace cass
