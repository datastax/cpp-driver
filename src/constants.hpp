/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_CONSTANTS_HPP_INCLUDED__
#define __CASS_CONSTANTS_HPP_INCLUDED__

#define CASS_UINT32_MAX 4294967295UL

#define CASS_INT64_MAX 9223372036854775807LL
#define CASS_INT64_MIN (-CASS_INT64_MAX - 1)

#define CQL_OPCODE_ERROR 0x00
#define CQL_OPCODE_STARTUP 0x01
#define CQL_OPCODE_READY 0x02
#define CQL_OPCODE_AUTHENTICATE 0x03
#define CQL_OPCODE_CREDENTIALS 0x04
#define CQL_OPCODE_OPTIONS 0x05
#define CQL_OPCODE_SUPPORTED 0x06
#define CQL_OPCODE_QUERY 0x07
#define CQL_OPCODE_RESULT 0x08
#define CQL_OPCODE_PREPARE 0x09
#define CQL_OPCODE_EXECUTE 0x0A
#define CQL_OPCODE_REGISTER 0x0B
#define CQL_OPCODE_EVENT 0x0C
#define CQL_OPCODE_BATCH 0x0D
#define CQL_OPCODE_AUTH_CHALLENGE 0x0E
#define CQL_OPCODE_AUTH_RESPONSE 0x0F
#define CQL_OPCODE_AUTH_SUCCESS 0x10
#define CQL_OPCODE_CANCEL 0xFF

// TODO(mpenick): We need to expose these (not as "CQL")
#define CQL_ERROR_SERVER_ERROR 0x0000
#define CQL_ERROR_PROTOCOL_ERROR 0x000A
#define CQL_ERROR_BAD_CREDENTIALS 0x0100
#define CQL_ERROR_UNAVAILABLE 0x1000
#define CQL_ERROR_OVERLOADED 0x1001
#define CQL_ERROR_IS_BOOTSTRAPPING 0x1002
#define CQL_ERROR_TRUNCATE_ERROR 0x1003
#define CQL_ERROR_WRITE_TIMEOUT 0x1100
#define CQL_ERROR_READ_TIMEOUT 0x1200
#define CQL_ERROR_READ_FAILURE 0x1300
#define CQL_ERROR_FUNCTION_FAILURE 0x1400
#define CQL_ERROR_WRITE_FAILURE 0x1500
#define CQL_ERROR_SYNTAX_ERROR 0x2000
#define CQL_ERROR_UNAUTHORIZED 0x2100
#define CQL_ERROR_INVALID_QUERY 0x2200
#define CQL_ERROR_CONFIG_ERROR 0x2300
#define CQL_ERROR_ALREADY_EXISTS 0x2400
#define CQL_ERROR_UNPREPARED 0x2500
#define CQL_ERROR_CLIENT_WRITE_FAILURE 0x8000

#define CASS_FLAG_COMPRESSION 0x01
#define CASS_FLAG_TRACING 0x02
#define CASS_FLAG_CUSTOM_PAYLOAD 0x04
#define CASS_FLAG_WARNING 0x08
#define CASS_FLAG_BETA 0x10

#define CASS_QUERY_FLAG_VALUES 0x00000001
#define CASS_QUERY_FLAG_SKIP_METADATA 0x00000002
#define CASS_QUERY_FLAG_PAGE_SIZE 0x00000004
#define CASS_QUERY_FLAG_PAGING_STATE 0x00000008
#define CASS_QUERY_FLAG_SERIAL_CONSISTENCY 0x00000010
#define CASS_QUERY_FLAG_DEFAULT_TIMESTAMP 0x00000020
#define CASS_QUERY_FLAG_NAMES_FOR_VALUES 0x00000040

#define CASS_BATCH_KIND_QUERY 0
#define CASS_BATCH_KIND_PREPARED 1

#define CASS_RESULT_KIND_VOID 1
#define CASS_RESULT_KIND_ROWS 2
#define CASS_RESULT_KIND_SET_KEYSPACE 3
#define CASS_RESULT_KIND_PREPARED 4
#define CASS_RESULT_KIND_SCHEMA_CHANGE 5

#define CASS_RESULT_FLAG_GLOBAL_TABLESPEC 0x00000001
#define CASS_RESULT_FLAG_HAS_MORE_PAGES 0x00000002
#define CASS_RESULT_FLAG_NO_METADATA 0x00000004

#define CASS_EVENT_TOPOLOGY_CHANGE 1
#define CASS_EVENT_STATUS_CHANGE 2
#define CASS_EVENT_SCHEMA_CHANGE 4

#define CASS_HEADER_SIZE_V1_AND_V2 8
#define CASS_HEADER_SIZE_V3 9

#define CASS_DEFAULT_CONSISTENCY CASS_CONSISTENCY_LOCAL_ONE
#define CASS_DEFAULT_SERIAL_CONSISTENCY CASS_CONSISTENCY_ANY
#define CASS_DEFAULT_REQUEST_TIMEOUT_MS 12000u

#define CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION 4
#define CASS_NEWEST_BETA_PROTOCOL_VERSION 5

#endif
