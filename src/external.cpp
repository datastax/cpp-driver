/*
  Copyright (c) DataStax, Inc.

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

#include "external.hpp"

#include "cassandra.h"

#include <string.h>
#include <uv.h>

#define NUM_SECONDS_PER_DAY (24U * 60U * 60U)
#define CASS_DATE_EPOCH 2147483648U // 2^31
#define CASS_TIME_NANOSECONDS_PER_SECOND 1000000000LL

extern "C" {

const char* cass_error_desc(CassError error) {
  switch (error) {
#define XX(source, _, code, desc) \
  case CASS_ERROR(source, code):  \
    return desc;
    CASS_ERROR_MAPPING(XX)
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
    CASS_LOG_LEVEL_MAPPING(XX)
#undef XX
    default:
      return "";
  }
}

const char* cass_consistency_string(CassConsistency consistency) {
  switch (consistency) {
#define XX(consistency, desc) \
  case consistency:           \
    return desc;
    CASS_CONSISTENCY_MAPPING(XX)
#undef XX
    default:
      return "";
  }
}

const char* cass_write_type_string(CassWriteType write_type) {
  switch (write_type) {
#define XX(write_type, desc) \
  case write_type:           \
    return desc;
    CASS_WRITE_TYPE_MAPPING(XX)
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

void cass_inet_string(CassInet inet, char* output) {
  uv_inet_ntop(inet.address_length == CASS_INET_V4_LENGTH ? AF_INET : AF_INET6, inet.address,
               output, CASS_INET_STRING_LENGTH);
}

CassError cass_inet_from_string(const char* str, CassInet* output) {
  if (uv_inet_pton(AF_INET, str, output->address) == 0) {
    output->address_length = CASS_INET_V4_LENGTH;
    return CASS_OK;
  } else if (uv_inet_pton(AF_INET6, str, output->address) == 0) {
    output->address_length = CASS_INET_V6_LENGTH;
    return CASS_OK;
  } else {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
}

CassError cass_inet_from_string_n(const char* str, size_t str_length, CassInet* output) {
  char buf[CASS_INET_STRING_LENGTH];
  // Need space for null terminator
  if (str_length > CASS_INET_STRING_LENGTH - 1) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  memcpy(buf, str, str_length);
  buf[str_length] = '\0';
  return cass_inet_from_string(buf, output);
}

cass_uint32_t cass_date_from_epoch(cass_int64_t epoch_secs) {
  return (epoch_secs / NUM_SECONDS_PER_DAY) + CASS_DATE_EPOCH;
}

cass_int64_t cass_time_from_epoch(cass_int64_t epoch_secs) {
  return CASS_TIME_NANOSECONDS_PER_SECOND * (epoch_secs % NUM_SECONDS_PER_DAY);
}

cass_int64_t cass_date_time_to_epoch(cass_uint32_t date, cass_int64_t time) {
  return (static_cast<cass_uint64_t>(date) - CASS_DATE_EPOCH) * NUM_SECONDS_PER_DAY +
         time / CASS_TIME_NANOSECONDS_PER_SECOND;
}

} // extern "C"
