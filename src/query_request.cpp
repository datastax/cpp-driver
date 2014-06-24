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

bool QueryRequest::prepare(size_t reserved, char** output, size_t& size) {
  uint8_t flags = 0x00;
  // reserved + the long string
  size = reserved + sizeof(int32_t) + query_.size();
  // consistency_value
  size += sizeof(int16_t);
  // flags
  size += 1;

  if (serial_consistency_ != 0) {
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
    size += sizeof(int16_t);
  }

  if (!paging_state_.empty()) {
    flags |= CASS_QUERY_FLAG_PAGING_STATE;
    size += (sizeof(int16_t) + paging_state_.size());
  }

  if (!is_empty()) {
    size += encoded_size();
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (page_size_ >= 0) {
    size += sizeof(int32_t);
    flags |= CASS_QUERY_FLAG_PAGE_SIZE;
  }

  if (serial_consistency_ != 0) {
    size += sizeof(int16_t);
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
  }

  *output = new char[size];

  char* buffer =
      encode_long_string(*output + reserved, query_.c_str(), query_.size());

  buffer = encode_short(buffer, consistency_);
  buffer = encode_byte(buffer, flags);

  if (!is_empty()) {
    buffer = encode(buffer);
  }

  if (page_size_ >= 0) {
    buffer = encode_int(buffer, page_size_);
  }

  if (!paging_state_.empty()) {
    buffer = encode_string(buffer, &paging_state_[0], paging_state_.size());
  }

  if (serial_consistency_ != 0) {
    buffer = encode_short(buffer, serial_consistency_);
  }

  return true;
}

} // namespace cass
