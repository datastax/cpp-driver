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

bool ExecuteRequest::encode(size_t reserved, char** output, size_t& size) {
  uint8_t flags = 0x00;

  // reserved + the short bytes
  size = reserved + sizeof(int16_t) + prepared_id_.size();
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

  char* buffer = encode_string(*output + reserved, prepared_id_.c_str(),
                               prepared_id_.size());

  buffer = encode_short(buffer, consistency());
  buffer = encode_byte(buffer, flags);

  if (has_values()) {
    buffer = encode_values(buffer);
  }

  if (page_size() >= 0) {
    buffer = encode_int(buffer, page_size());
  }

  if (!paging_state().empty()) {
    buffer = encode_string(buffer, paging_state().data(), paging_state().size());
  }

  if (serial_consistency() != 0) {
    buffer = encode_short(buffer, serial_consistency());
  }

  return true;
}

} // namespace cass
