/*
  Copyright (c) 2014 DataStax

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

#include "cassandra.hpp"
#include "collection.hpp"

extern "C" {

cass_collection_t*
cass_collection_new(size_t element_count) {
  return cass_collection_t::to(new cass::Collection(element_count));
}

void
cass_collection_free(cass_collection_t* collection) {
  delete collection->from();
}

cass_code_t
cass_collection_append_int32(cass_collection_t* collection,
                             cass_int32_t i32) {
  collection->append_int32(i32);
  return CASS_OK;
}

cass_code_t
cass_collection_append_int64(cass_collection_t* collection,
                             cass_int32_t i64) {
  collection->append_int64(i64);
  return CASS_OK;
}

cass_code_t
cass_collection_append_float(cass_collection_t* collection,
                             cass_float_t f) {
  collection->append_float(f);
  return CASS_OK;
}

cass_code_t
cass_collection_append_double(cass_collection_t* collection,
                              cass_double_t d) {
  collection->append_double(d);
  return CASS_OK;
}

cass_code_t
cass_collection_append_bool(cass_collection_t* collection,
                            cass_bool_t b) {
  collection->append_bool(b);
  return CASS_OK;
}

cass_code_t
cass_collection_append_inet(cass_collection_t* collection,
                            cass_inet_t inet) {
  collection->append(inet.address, inet.address_len);
  return CASS_OK;
}

cass_code_t
cass_collection_append_decimal(cass_collection_t* collection,
                               cass_int32_t scale,
                               const cass_uint8_t* varint, cass_size_t varint_length) {
  // TODO(mpenick)
  return CASS_OK;
}

cass_code_t
cass_collection_append_uuid(cass_collection_t* collection,
                            cass_uuid_t uuid) {
  collection->append(uuid);
  return CASS_OK;
}

cass_code_t
cass_collection_append_blob(cass_collection_t* collection,
                             const cass_uint8_t* blob, cass_size_t blob_length) {
  collection->append(blob, blob_length);
  return CASS_OK;
}

} // extern "C"
