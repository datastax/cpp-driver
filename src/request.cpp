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

#include "request.hpp"

#include "external_types.hpp"

extern "C" {

CassCustomPayload* cass_custom_payload_new(size_t item_count) {
  cass::CustomPayload* payload = new cass::CustomPayload(item_count);
  payload->inc_ref();
  return CassCustomPayload::to(payload);
}


void cass_custom_payload_append(CassCustomPayload* payload,
                                const char* name,
                                const cass_byte_t* value,
                                size_t value_size) {
  payload->append(name, strlen(name), value, value_size);
}

void cass_custom_payload_append_n(CassCustomPayload* payload,
                                  const char* name,
                                  size_t name_length,
                                  const cass_byte_t* value,
                                  size_t value_size) {
  payload->append(name, name_length, value, value_size);
}

void cass_custom_payload_free(CassCustomPayload* payload) {
  payload->dec_ref();
}

} // extern "C"

namespace cass {

void CustomPayload::append(const char* name, size_t name_length, const uint8_t* value, size_t value_size) {
  Buffer buf(sizeof(uint16_t) + name_length + sizeof(int32_t) + value_size);
  size_t pos = buf.encode_string(0, name, name_length);
  buf.encode_bytes(pos, reinterpret_cast<const char*>(value), value_size);
  items_.push_back(buf);
}

int32_t CustomPayload::encode(BufferVec* bufs) const {
  int32_t length = sizeof(uint16_t);
  Buffer buf(sizeof(uint16_t));
  buf.encode_uint16(0, items_.size());
  bufs->push_back(buf);
  for (ItemVec::const_iterator i = items_.begin(), end = items_.end();
       i != end;
       ++i) {
    length += i->size();
    bufs->push_back(*i);
  }
  return length;
}

} // namespace cass
