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

#include "execute_request.hpp"

#include "constants.hpp"
#include "protocol.hpp"
#include "request_callback.hpp"

using namespace datastax::internal::core;

ExecuteRequest::ExecuteRequest(const Prepared* prepared)
    : Statement(prepared)
    , prepared_(prepared) {}

int ExecuteRequest::encode(ProtocolVersion version, RequestCallback* callback,
                           BufferVec* bufs) const {
  int32_t length = encode_query_or_id(bufs);
  if (version.supports_result_metadata_id()) {
    if (callback->prepared_metadata_entry()) {
      const Buffer& result_metadata_id(callback->prepared_metadata_entry()->result_metadata_id());
      bufs->push_back(result_metadata_id);
      length += result_metadata_id.size();
    } else {
      bufs->push_back(Buffer(sizeof(uint16_t)));
      bufs->back().encode_uint16(0, 0);
      length += bufs->back().size();
    }
  }
  length += encode_begin(version, static_cast<uint16_t>(elements().size()), callback, bufs);
  int32_t result = encode_values(version, callback, bufs);
  if (result < 0) return result;
  length += result;
  length += encode_end(version, callback, bufs);
  return length;
}
