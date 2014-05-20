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

extern "C" {

const char*
cass_code_error_desc(CassError code) {
  return ""; // TODO(mpenick)
}

CassString cass_string_init(const char* null_terminated) {
  CassString str;
  str.data = null_terminated;
  str.length = strlen(null_terminated);
  return str;
}

CassString cass_string_init2(const char* data, cass_size_t length) {
  CassString str;
  str.data = data;
  str.length = length;
  return str;
}

CassBytes cass_bytes_init(cass_byte_t* data, cass_size_t size) {
  CassBytes bytes;
  bytes.data = data;
  bytes.size = size;
  return bytes;
}


} // extern "C"
