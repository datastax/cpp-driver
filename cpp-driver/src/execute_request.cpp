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

int32_t ExecuteRequest::encode_batch(int version, BufferVec* bufs, RequestCallback* callback) const {
  int32_t length = 0;
  const std::string& id(prepared_->id());

  // <kind><id><n><value_1>...<value_n> ([byte][short bytes][short][bytes]...[bytes])
  int buf_size = sizeof(uint8_t) + sizeof(uint16_t) + id.size() + sizeof(uint16_t);

  bufs->push_back(Buffer(buf_size));
  length += buf_size;

  Buffer& buf = bufs->back();
  size_t pos = buf.encode_byte(0, kind());
  pos = buf.encode_string(pos, id.data(), id.size());

  buf.encode_uint16(pos, elements_count());
  if (elements_count() > 0) {
    int32_t result = copy_buffers(version, bufs, callback);
    if (result < 0) return result;
    length += result;
  }

  return length;
}

int ExecuteRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  if (version == 1) {
    return internal_encode_v1(callback, bufs);
  } else {
    return internal_encode(version, callback, bufs);
  }
}

int ExecuteRequest::internal_encode_v1(RequestCallback* callback, BufferVec* bufs) const {
  size_t length = 0;
  const int version = 1;

  const std::string& prepared_id = prepared_->id();

    // <id> [short bytes] + <n> [short]
  size_t prepared_buf_size = sizeof(uint16_t) + prepared_id.size() +
                             sizeof(uint16_t);

  {
    bufs->push_back(Buffer(prepared_buf_size));
    length += prepared_buf_size;

    Buffer& buf = bufs->back();
    size_t pos = buf.encode_string(0,
                                 prepared_id.data(),
                                 prepared_id.size());
    buf.encode_uint16(pos, elements_count());
    // <value_1>...<value_n>
    int32_t result = copy_buffers(version, bufs, callback);
    if (result < 0) return result;
    length += result;
  }

  {
    // <consistency> [short]
    size_t buf_size = sizeof(uint16_t);

    Buffer buf(buf_size);
    buf.encode_uint16(0, callback->consistency());
    bufs->push_back(buf);
    length += buf_size;
  }

  return length;
}

int ExecuteRequest::internal_encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  int length = 0;
  uint8_t flags = this->flags();

  const std::string& prepared_id = prepared_->id();

    // <id> [short bytes] + <consistency> [short] + <flags> [byte]
  size_t prepared_buf_size = sizeof(uint16_t) + prepared_id.size() +
                          sizeof(uint16_t) + sizeof(uint8_t);
  size_t paging_buf_size = 0;

  if (elements_count() > 0) { // <values> = <n><value_1>...<value_n>
    prepared_buf_size += sizeof(uint16_t); // <n> [short]
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (page_size() >= 0) {
    paging_buf_size += sizeof(int32_t); // [int]
    flags |= CASS_QUERY_FLAG_PAGE_SIZE;
  }

  if (!paging_state().empty()) {
    paging_buf_size += sizeof(int32_t) + paging_state().size(); // [bytes]
    flags |= CASS_QUERY_FLAG_PAGING_STATE;
  }

  if (serial_consistency() != 0) {
    paging_buf_size += sizeof(uint16_t); // [short]
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
  }

  if (version >= 3 && callback->timestamp() != CASS_INT64_MIN) {
    paging_buf_size += sizeof(int64_t); // [long]
    flags |= CASS_QUERY_FLAG_DEFAULT_TIMESTAMP;
  }

  {
    bufs->push_back(Buffer(prepared_buf_size));
    length += prepared_buf_size;

    Buffer& buf = bufs->back();
    size_t pos = buf.encode_string(0,
                                 prepared_id.data(),
                                 prepared_id.size());
    pos = buf.encode_uint16(pos, callback->consistency());
    pos = buf.encode_byte(pos, flags);

    if (elements_count() > 0) {
      buf.encode_uint16(pos, elements_count());
      int32_t result = copy_buffers(version, bufs, callback);
      if (result < 0) return result;
      length += result;
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

    if (version >= 3 && callback->timestamp() != CASS_INT64_MIN) {
      pos = buf.encode_int64(pos, callback->timestamp());
    }
  }

  return length;
}

} // namespace cass
