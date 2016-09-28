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

#include "query_request.hpp"

#include "constants.hpp"
#include "request_callback.hpp"
#include "logger.hpp"
#include "serialization.hpp"

namespace cass {

int32_t QueryRequest::encode_batch(int version, BufferVec* bufs, RequestCallback* callback) const {
  int32_t length = 0;
  const std::string& query(query_);

  // <kind><string><n>[name_1]<value_1>...[name_n]<value_n> ([byte][long string][short][bytes]...[bytes])
  int buf_size = sizeof(uint8_t) + sizeof(int32_t) + query.size() + sizeof(uint16_t);

  bufs->push_back(Buffer(buf_size));
  length += buf_size;

  Buffer& buf = bufs->back();
  size_t pos = buf.encode_byte(0, kind());
  pos = buf.encode_long_string(pos, query.data(), query.size());

  if (has_names_for_values()) {
    if (version < 3) {
      LOG_ERROR("Protocol version %d does not support named values", version);
      return REQUEST_ERROR_UNSUPPORTED_PROTOCOL;
    }
    buf.encode_uint16(pos, value_names_.size());
    length += copy_buffers_with_names(version, bufs, callback->encoding_cache());
  } else {
    buf.encode_uint16(pos, elements_count());
    if (elements_count() > 0) {
      int32_t result = copy_buffers(version, bufs, callback);
      if (result < 0) return result;
      length += result;
    }
  }

  return length;
}

size_t QueryRequest::get_indices(StringRef name, IndexVec* indices) {
  set_has_names_for_values(true);

  if (value_names_.get_indices(name, indices) == 0) {
    if (value_names_.size() > elements_count()) {
      // No more space left for new named values
      return 0;
    }
    if (name.size() > 0 && name.front() == '"' && name.back() == '"') {
      name = name.substr(1, name.size() - 2);
    }
    indices->push_back(value_names_.add(ValueName(name.to_string())));
  }

  return indices->size();
}

int32_t QueryRequest::copy_buffers_with_names(int version,
                                              BufferVec* bufs,
                                              EncodingCache* cache) const {
  int32_t size = 0;
  for (size_t i = 0; i < value_names_.size(); ++i) {
    const Buffer& name_buf = value_names_[i].buf;
    bufs->push_back(name_buf);

    Buffer value_buf(elements()[i].get_buffer_cached(version, cache, false));
    bufs->push_back(value_buf);

    size += name_buf.size() + value_buf.size();
  }
  return size;
}

int QueryRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  if (version == 1) {
    return internal_encode_v1(callback, bufs);
  } else  {
    return internal_encode(version, callback, bufs);
  }
}

int QueryRequest::internal_encode_v1(RequestCallback* callback, BufferVec* bufs) const {
  // <query> [long string] + <consistency> [short]
  size_t length = sizeof(int32_t) + query_.size() + sizeof(uint16_t);

  Buffer buf(length);
  size_t pos = buf.encode_long_string(0, query_.data(), query_.size());
  buf.encode_uint16(pos, callback->consistency());
  bufs->push_back(buf);

  return length;
}

int QueryRequest::internal_encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  int length = 0;
  uint8_t flags = this->flags();

    // <query> [long string] + <consistency> [short] + <flags> [byte]
  size_t query_buf_size = sizeof(int32_t) + query_.size() +
                          sizeof(uint16_t) + sizeof(uint8_t);
  size_t paging_buf_size = 0;

  if (elements_count() > 0) { // <values> = <n><value_1>...<value_n>
    query_buf_size += sizeof(uint16_t); // <n> [short]
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (page_size() > 0) {
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
    bufs->push_back(Buffer(query_buf_size));
    length += query_buf_size;

    Buffer& buf = bufs->back();
    size_t pos = buf.encode_long_string(0, query_.data(), query_.size());
    pos = buf.encode_uint16(pos, callback->consistency());
    pos = buf.encode_byte(pos, flags);

    if (has_names_for_values()) {
      if (version < 3) {
        LOG_ERROR("Protocol version %d does not support named values", version);
        return REQUEST_ERROR_UNSUPPORTED_PROTOCOL;
      }
      buf.encode_uint16(pos, value_names_.size());
      length += copy_buffers_with_names(version, bufs, callback->encoding_cache());
    } else if (elements_count() > 0) {
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
