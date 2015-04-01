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

#include "buffer_collection.hpp"

#include "types.hpp"


extern "C" {

CassCollection* cass_collection_new(CassCollectionType type, size_t element_count) {
  cass::BufferCollection* collection = new cass::BufferCollection(type == CASS_COLLECTION_TYPE_MAP,
                                                                 element_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

void cass_collection_free(CassCollection* collection) {
  collection->dec_ref();
}

CassError cass_collection_append_int32(CassCollection* collection,
                                       cass_int32_t value) {
  collection->append_int32(value);
  return CASS_OK;
}

CassError cass_collection_append_int64(CassCollection* collection,
                                       cass_int64_t value) {
  collection->append_int64(value);
  return CASS_OK;
}

CassError cass_collection_append_float(CassCollection* collection,
                                       cass_float_t value) {
  collection->append_float(value);
  return CASS_OK;
}

CassError cass_collection_append_double(CassCollection* collection,
                                        cass_double_t value) {
  collection->append_double(value);
  return CASS_OK;
}

CassError cass_collection_append_bool(CassCollection* collection,
                                      cass_bool_t value) {
  collection->append_byte(value == cass_true);
  return CASS_OK;
}

CassError cass_collection_append_string(CassCollection* collection,
                                        const char* value) {
  return cass_collection_append_string_n(collection, value, strlen(value));
}

CassError cass_collection_append_string_n(CassCollection* collection,
                                          const char* value,
                                          size_t value_length) {
  collection->append(value, value_length);
  return CASS_OK;
}

CassError cass_collection_append_bytes(CassCollection* collection,
                                       const cass_byte_t* value,
                                       size_t value_size) {
  collection->append(value, value_size);
  return CASS_OK;
}

CassError cass_collection_append_uuid(CassCollection* collection,
                                      CassUuid value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_inet(CassCollection* collection,
                                      CassInet value) {
  collection->append(value.address, value.address_length);
  return CASS_OK;
}

CassError cass_collection_append_decimal(CassCollection* collection,
                                         const cass_byte_t* varint,
                                         size_t varint_size,
                                         cass_int32_t scale) {
  collection->append(varint, varint_size, scale);
  return CASS_OK;
}

} // extern "C"

namespace cass {

int BufferCollection::encode(int version, BufferVec* bufs) const {
  if (version != 1 && version != 2) return -1;

  int value_size = sizeof(uint16_t) + calculate_size(version);
  int buf_size = sizeof(int32_t) + value_size;

  Buffer buf(buf_size);

  int pos = 0;
  pos = buf.encode_int32(pos, value_size);
  pos = buf.encode_uint16(pos, is_map_ ? bufs_.size() / 2 : bufs_.size());

  encode(version, buf.data() + pos);

  bufs->push_back(buf);

  return buf_size;
}

int BufferCollection::calculate_size(int version) const {
  if (version != 1 && version != 2) return -1;
  int value_size = 0;
  for (BufferVec::const_iterator it = bufs_.begin(),
      end = bufs_.end(); it != end; ++it) {
    value_size += sizeof(uint16_t);
    value_size += it->size();
  }
  return value_size;
}

void BufferCollection::encode(int version, char* buf) const {
  assert(version == 1 || version == 2);
  char* pos = buf;
  for (BufferVec::const_iterator it = bufs_.begin(),
      end = bufs_.end(); it != end; ++it) {
    encode_uint16(pos, it->size());
    pos += sizeof(uint16_t);

    memcpy(pos, it->data(), it->size());
    pos += it->size();
  }
}

}
