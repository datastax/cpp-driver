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

CassCollection* cass_collection_new(size_t element_count) {
  return CassCollection::to(new cass::Collection(element_count));
}

void cass_collection_free(CassCollection* collection) {
  delete collection->from();
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
  collection->append_bool(value == cass_true);
  return CASS_OK;
}

CassError cass_collection_append_string(CassCollection* collection,
                                        CassString value) {
  collection->append(value.data, value.length);
  return CASS_OK;
}

CassError cass_collection_append_bytes(CassCollection* collection,
                                       CassBytes value) {
  collection->append(value.data, value.size);
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
                                         CassDecimal value) {
  collection->append(value.scale, value.varint.data, value.varint.size);
  return CASS_OK;
}

} // extern "C"
