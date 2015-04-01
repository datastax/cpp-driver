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

#include "types.hpp"

extern "C" {

const char* cass_error_desc(CassError error) {
  switch (error) {
#define XX(source, _, code, desc) \
  case CASS_ERROR(source, code):  \
    return desc;
    CASS_ERROR_MAP(XX)
#undef XX
    default:
      return "";
  }
}

const char* cass_log_level_string(CassLogLevel log_level) {
  switch (log_level) {
#define XX(log_level, desc) \
  case log_level:           \
    return desc;
    CASS_LOG_LEVEL_MAP(XX)
#undef XX
    default:
      return "";
  }
}

CassInet cass_inet_init_v4(const cass_uint8_t* address) {
  CassInet inet;
  inet.address_length = CASS_INET_V4_LENGTH;
  memcpy(inet.address, address, CASS_INET_V4_LENGTH);
  return inet;
}

CassInet cass_inet_init_v6(const cass_uint8_t* address) {
  CassInet inet;
  inet.address_length = CASS_INET_V6_LENGTH;
  memcpy(inet.address, address, CASS_INET_V6_LENGTH);
  return inet;
}

} // extern "C"
