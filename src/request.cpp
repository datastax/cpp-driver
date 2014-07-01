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

RequestMessage* RequestMessage::create(Request* request) {
  switch(request->opcode()) {
    case CQL_OPCODE_PREPARE:
      return new PrepareRequestMessage(request);

    case CQL_OPCODE_OPTIONS:
      return new OptionsRequestMessage(request);

    case CQL_OPCODE_STARTUP:
      return new StartupRequestMessage(request);

    case CQL_OPCODE_QUERY:
      return new QueryRequestMessage(request);

    case CQL_OPCODE_EXECUTE:
      return new ExecuteRequestMessage(request);

    case CQL_OPCODE_BATCH:
      return new BatchRequestMessage(request);

    default:
      assert(false && "Invalid request type");
      return NULL;
  }
}

bool RequestMessage::encode(int version, int flags, int stream, Writer::Bufs* bufs) {
  if(version == 1 || version == 2) {
    header_buf_ = Buffer(CASS_HEADER_SIZE_V1_AND_V2);

    char* data = header_buf_.data();

    data[0] = version;
    data[1] = flags;
    data[2] = stream;
    data[3] = opcode();

    bufs->push_back(uv_buf_init(header_buf_.data(), header_buf_.size()));

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
