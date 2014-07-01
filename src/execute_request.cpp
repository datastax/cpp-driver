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

#include "execute_request.hpp"

namespace cass {

int32_t ExecuteRequestMessage::encode(int version, Writer::Bufs* bufs) {
  assert(version == 2);

  uint8_t flags = 0;
  int32_t length = 0;

  const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request());

  {
    // <id> [short bytes] + <consistency> [short] + <flags> [byte]
    int32_t buf_size = 2 + execute->prepared_id().size() + 2 + 1;

    if (execute->has_values()) { // <values> = <n><value_1>...<value_n>
      buf_size += 2; // <n> [short]
    }

    body_head_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_head_buf_.data(), body_head_buf_.size()));

    char* pos = encode_string(body_head_buf_.data(),
                              execute->prepared_id().data(),
                              execute->prepared_id().size());
    pos = encode_short(pos, execute->consistency());
    pos = encode_byte(pos, flags);

    if (execute->has_values()) {
      encode_short(pos, execute->values_count());
      length += execute->encode_values(version, &body_collection_bufs_, bufs);
    }
  }

  {
    int32_t buf_size = 0;

    if (execute->page_size() >= 0) {
      buf_size += 4; // [int]
    }

    if (!execute->paging_state().empty()) {
      buf_size += execute->paging_state().size(); // [bytes]
    }

    if (execute->serial_consistency() != 0) {
      buf_size += 2; // [short]
    }

    body_tail_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_tail_buf_.data(), body_tail_buf_.size()));

    char* pos = body_tail_buf_.data();

    if (execute->page_size() >= 0) {
      pos = encode_int(pos, execute->page_size());
    }

    if (!execute->paging_state().empty()) {
      pos = encode_string(pos, execute->paging_state().data(), execute->paging_state().size());
    }

    if (execute->serial_consistency() != 0) {
      pos = encode_short(pos, execute->serial_consistency());
    }
  }

  return length;
}

} // namespace cass
