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

#include "execute_request.hpp"

#include "constants.hpp"
#include "request_callback.hpp"

namespace cass {

ExecuteRequest::ExecuteRequest(const Prepared* prepared)
  : Statement(prepared)
  , prepared_(prepared) { }

int ExecuteRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  if (version == 1) {
    return encode_v1(callback, bufs);
  } else {
    int32_t length = encode_query_or_id(bufs);
    if (version >= CASS_PROTOCOL_VERSION_V5 &&
        callback->prepared_metadata_entry()) {
      const Buffer& result_metadata_id(callback->prepared_metadata_entry()->result_metadata_id());
      bufs->push_back(result_metadata_id);
      length += result_metadata_id.size();
    }
    length += encode_begin(version, elements().size(), callback, bufs);
    int32_t result = encode_values(version, callback, bufs);
    if (result < 0) return result;
    length += result;
    length += encode_end(version, callback, bufs);
    return length;
  }
}

} // namespace cass
