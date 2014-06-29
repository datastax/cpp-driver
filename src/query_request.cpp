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


int32_t QueryRequest::encode(int version, BufferVec* bufs) {
  assert(version == 2);

  uint8_t flags = 0;
  int32_t length = 0;

  {
    // <query> [long string] + <consistency> [short] + <flags> [byte]
    int32_t buf_size = 4 + query_.size() + 2 + 1;

    if (has_values()) { // <values> = <n><value_1>...<value_n>
      buf_size += 2; // <n> [short]
    }

    Buffer buf(buf_size);

    length += buf_size;

    bufs->push_back(buf);

    char* pos = encode_long_string(buf.data(), query_.c_str(), query_.size());
    pos = encode_short(pos, consistency());
    pos = encode_byte(pos, flags);

    if (has_values()) {
      encode_short(pos, size());
      length += encode_values(bufs);
    }
  }

  {
    int32_t buf_size = 0;

    if (page_size() >= 0) {
      buf_size += 4; // [int]
    }

    if (!paging_state().empty()) {
      buf_size += paging_state().size(); // [bytes]
    }

    if (serial_consistency() != 0) {
      buf_size += 2; // [short]
    }

    Buffer buf(buf_size);

    length += buf_size;

    char* pos = buf.data();

    if (page_size() >= 0) {
      pos = encode_int(pos, page_size());
    }

    if (!paging_state().empty()) {
      pos = encode_string(pos, paging_state().data(), paging_state().size());
    }

    if (serial_consistency() != 0) {
      pos = encode_short(pos, serial_consistency());
    }
  }

  return length;
}

bool QueryRequest::encode(size_t reserved, char** output, size_t& size) {
  uint8_t flags = 0x00;
  // reserved + the long string
  size = reserved + sizeof(int32_t) + query_.size();
  // consistency_value
  size += sizeof(int16_t);
  // flags
  size += 1;

  if (serial_consistency() != 0) {
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
    size += sizeof(int16_t);
  }

  if (!paging_state().empty()) {
    flags |= CASS_QUERY_FLAG_PAGING_STATE;
    size += (sizeof(int16_t) + paging_state().size());
  }

  if (has_values()) {
    size += encoded_values_size();
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (page_size() >= 0) {
    size += sizeof(int32_t);
    flags |= CASS_QUERY_FLAG_PAGE_SIZE;
  }

  if (serial_consistency() != 0) {
    size += sizeof(int16_t);
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
  }

  *output = new char[size];

  char* buffer =
      encode_long_string(*output + reserved, query_.c_str(), query_.size());

  buffer = encode_short(buffer, consistency());
  buffer = encode_byte(buffer, flags);

  if (has_values()) {
    buffer = encode_values(buffer);
  }

  if (page_size() >= 0) {
    buffer = encode_int(buffer, page_size());
  }

  if (!paging_state().empty()) {
    buffer = encode_string(buffer, &paging_state()[0], paging_state().size());
  }

  if (serial_consistency() != 0) {
    buffer = encode_short(buffer, serial_consistency());
  }

  return true;
}

} // namespace cass
