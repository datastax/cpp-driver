/*
  Copyright (c) 2014-2015 DataStax

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

int QueryRequest::encode(int version, BufferVec* bufs) const {
  if (version == 1) {
    return encode_v1(bufs);
  } else if (version == 2) {
    return encode_v2(bufs);
  } else {
    return ENCODE_ERROR_UNSUPPORTED_PROTOCOL;
  }
}

int QueryRequest::encode_v1(BufferVec* bufs) const {
  // <query> [long string] + <consistency> [short]
  size_t length = sizeof(int32_t) + query().size() + sizeof(uint16_t);

  Buffer buf(length);
  size_t pos = buf.encode_long_string(0, query().data(), query().size());
  buf.encode_uint16(pos, consistency());
  bufs->push_back(buf);

  return length;
}

int QueryRequest::encode_v2(BufferVec* bufs) const {
  const int version = 2;

  uint8_t flags = 0;
  size_t length = 0;

    // <query> [long string] + <consistency> [short] + <flags> [byte]
  size_t query_buf_size = sizeof(int32_t) + query().size() +
                          sizeof(uint16_t) + sizeof(uint8_t);
  size_t paging_buf_size = 0;

  if (values_count() > 0) { // <values> = <n><value_1>...<value_n>
    query_buf_size += sizeof(uint16_t); // <n> [short]
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (skip_metadata()) {
    flags |= CASS_QUERY_FLAG_SKIP_METADATA;
  }

  if (page_size() > 0) {
    paging_buf_size += sizeof(int32_t); // [int]
    flags |= CASS_QUERY_FLAG_PAGE_SIZE;
  } else {

  }

  if (!paging_state().empty()) {
    paging_buf_size += sizeof(int32_t) + paging_state().size(); // [bytes]
    flags |= CASS_QUERY_FLAG_PAGING_STATE;
  }

  if (serial_consistency() != 0) {
    paging_buf_size += sizeof(uint16_t); // [short]
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
  }

  {
    bufs->push_back(Buffer(query_buf_size));
    length += query_buf_size;

    Buffer& buf = bufs->back();
    size_t pos = buf.encode_long_string(0, query().data(), query().size());
    pos = buf.encode_uint16(pos, consistency());
    pos = buf.encode_byte(pos, flags);

    if (values_count() > 0) {
      buf.encode_uint16(pos, values_count());
      length += encode_values(version, bufs);
    }
  }

  if (paging_buf_size > 0) {
    bufs->push_back(Buffer(paging_buf_size));
    length += paging_buf_size;

    Buffer& buf = bufs->back();
    size_t pos = 0;

    if (page_size() >= 0) {
      pos = buf.encode_int32(pos, page_size());
    }

    if (!paging_state().empty()) {
      pos = buf.encode_bytes(pos, paging_state().data(), paging_state().size());
    }

    if (serial_consistency() != 0) {
      pos = buf.encode_uint16(pos, serial_consistency());
    }
  }

  return length;
}

} // namespace cass
