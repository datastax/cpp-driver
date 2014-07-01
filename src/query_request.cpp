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

#include "query_request.hpp"
#include "serialization.hpp"

namespace cass {

int32_t QueryRequestMessage::encode(int version, Writer::Bufs* bufs) {
  assert(version == 2);

  uint8_t flags = 0;
  int32_t length = 0;

  const QueryRequest* query = static_cast<const QueryRequest*>(request());

  {
    // <query> [long string] + <consistency> [short] + <flags> [byte]
    int32_t buf_size = 4 + query->query().size() + 2 + 1;

    if (query->has_values()) { // <values> = <n><value_1>...<value_n>
      buf_size += 2; // <n> [short]
    }

    body_head_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_head_buf_.data(), body_head_buf_.size()));

    char* pos = encode_long_string(body_head_buf_.data(), query->query().data(), query->query().size());
    pos = encode_short(pos, query->consistency());
    pos = encode_byte(pos, flags);

    if (query->has_values()) {
      encode_short(pos, query->values_count());      
      length += query->encode_values(version, &body_collection_bufs_, bufs);
    }
  }

  {
    int32_t buf_size = 0;

    if (query->page_size() >= 0) {
      buf_size += 4; // [int]
    }

    if (!query->paging_state().empty()) {
      buf_size += query->paging_state().size(); // [bytes]
    }

    if (query->serial_consistency() != 0) {
      buf_size += 2; // [short]
    }

    body_tail_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_tail_buf_.data(), body_tail_buf_.size()));

    char* pos = body_tail_buf_.data();

    if (query->page_size() >= 0) {
      pos = encode_int(pos, query->page_size());
    }

    if (!query->paging_state().empty()) {
      pos = encode_string(pos, query->paging_state().data(), query->paging_state().size());
    }

    if (query->serial_consistency() != 0) {
      pos = encode_short(pos, query->serial_consistency());
    }
  }

  return length;
}

} // namespace cass
