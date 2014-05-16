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
                             cass_int32_t value) {
  collection->append_int32(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_int64(cass_collection_t* collection,
                             cass_int64_t value) {
  collection->append_int64(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_float(cass_collection_t* collection,
                             cass_float_t value) {
  collection->append_float(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_double(cass_collection_t* collection,
                              cass_double_t value) {
  collection->append_double(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_bool(cass_collection_t* collection,
                            cass_bool_t value) {
  collection->append_bool(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_string(cass_collection_t* collection,
                              const char* value, cass_size_t value_length) {
  collection->append(value, value_length);
  return CASS_OK;
}

cass_code_t
cass_collection_append_bytes(cass_collection_t* collection,
                             const cass_byte_t* value, cass_size_t value_length) {
  collection->append(value, value_length);
  return CASS_OK;
}

cass_code_t
cass_collection_append_uuid(cass_collection_t* collection,
                            cass_uuid_t value) {
  collection->append(value);
  return CASS_OK;
}

cass_code_t
cass_collection_append_inet(cass_collection_t* collection,
                            cass_inet_t value) {
  collection->append(value.address, value.address_length);
  return CASS_OK;
}

cass_code_t
cass_collection_append_decimal(cass_collection_t* collection,
                               cass_int32_t scale,
                               const cass_byte_t* varint, cass_size_t varint_length) {
  collection->append(scale, varint, varint_length);
  return CASS_OK;
}

} // extern "C"
