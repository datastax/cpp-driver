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

#include "pretty_print.hpp"

void PrintTo(CassConsistency consistency, std::ostream* output_stream) {
  switch (consistency) {
    case CASS_CONSISTENCY_UNKNOWN:
      *output_stream << "CASS_CONSISTENCY_UNKNOWN";
      break;
    case CASS_CONSISTENCY_ANY:
      *output_stream << "CASS_CONSISTENCY_ANY";
      break;
    case CASS_CONSISTENCY_ONE:
      *output_stream << "CASS_CONSISTENCY_ONE";
      break;
    case CASS_CONSISTENCY_TWO:
      *output_stream << "CASS_CONSISTENCY_TWO";
      break;
    case CASS_CONSISTENCY_THREE:
      *output_stream << "CASS_CONSISTENCY_THREE";
      break;
    case CASS_CONSISTENCY_QUORUM:
      *output_stream << "CASS_CONSISTENCY_QUORUM";
      break;
    case CASS_CONSISTENCY_ALL:
      *output_stream << "CASS_CONSISTENCY_ALL";
      break;
    case CASS_CONSISTENCY_LOCAL_QUORUM:
      *output_stream << "CASS_CONSISTENCY_LOCAL_QUORUM";
      break;
    case CASS_CONSISTENCY_EACH_QUORUM:
      *output_stream << "CASS_CONSISTENCY_EACH_QUORUM";
      break;
    case CASS_CONSISTENCY_SERIAL:
      *output_stream << "CASS_CONSISTENCY_SERIAL";
      break;
    case CASS_CONSISTENCY_LOCAL_SERIAL:
      *output_stream << "CASS_CONSISTENCY_LOCAL_SERIAL";
      break;
    case CASS_CONSISTENCY_LOCAL_ONE:
      *output_stream << "CASS_CONSISTENCY_LOCAL_ONE";
      break;
    default:
      *output_stream << "CASS CONSISTENCY NEEDS TO BE ADDED";
  }
}

void PrintTo(CassError error_code, std::ostream* output_stream) {
  switch (error_code) {
    case CASS_OK:
      *output_stream << "CASS_OK";
      break;
    case CASS_ERROR_LIB_BAD_PARAMS:
      *output_stream << "CASS_ERROR_LIB_BAD_PARAMS";
      break;
    case CASS_ERROR_LIB_NO_STREAMS:
      *output_stream << "CASS_ERROR_LIB_NO_STREAMS";
      break;
    case CASS_ERROR_LIB_UNABLE_TO_INIT:
      *output_stream << "CASS_ERROR_LIB_UNABLE_TO_INIT";
      break;
    case CASS_ERROR_LIB_MESSAGE_ENCODE:
      *output_stream << "CASS_ERROR_LIB_MESSAGE_ENCODE";
      break;
    case CASS_ERROR_LIB_HOST_RESOLUTION:
      *output_stream << "CASS_ERROR_LIB_HOST_RESOLUTION";
      break;
    case CASS_ERROR_LIB_UNEXPECTED_RESPONSE:
      *output_stream << "CASS_ERROR_LIB_UNEXPECTED_RESPONSE";
      break;
    case CASS_ERROR_LIB_REQUEST_QUEUE_FULL:
      *output_stream << "CASS_ERROR_LIB_REQUEST_QUEUE_FULL";
      break;
    case CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD:
      *output_stream << "CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD";
      break;
    case CASS_ERROR_LIB_WRITE_ERROR:
      *output_stream << "CASS_ERROR_LIB_WRITE_ERROR";
      break;
    case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
      *output_stream << "CASS_ERROR_LIB_NO_HOSTS_AVAILABLE";
      break;
    case CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS:
      *output_stream << "CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS";
      break;
    case CASS_ERROR_LIB_INVALID_ITEM_COUNT:
      *output_stream << "CASS_ERROR_LIB_INVALID_ITEM_COUNT";
      break;
    case CASS_ERROR_LIB_INVALID_VALUE_TYPE:
      *output_stream << "CASS_ERROR_LIB_INVALID_VALUE_TYPE";
      break;
    case CASS_ERROR_LIB_REQUEST_TIMED_OUT:
      *output_stream << "CASS_ERROR_LIB_REQUEST_TIMED_OUT";
      break;
    case CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE:
      *output_stream << "CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE";
      break;
    case CASS_ERROR_LIB_CALLBACK_ALREADY_SET:
      *output_stream << "CASS_ERROR_LIB_CALLBACK_ALREADY_SET";
      break;
    case CASS_ERROR_LIB_INVALID_STATEMENT_TYPE:
      *output_stream << "CASS_ERROR_LIB_INVALID_STATEMENT_TYPE";
      break;
    case CASS_ERROR_LIB_NAME_DOES_NOT_EXIST:
      *output_stream << "CASS_ERROR_LIB_NAME_DOES_NOT_EXIST";
      break;
    case CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL:
      *output_stream << "CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL";
      break;
    case CASS_ERROR_LIB_NULL_VALUE:
      *output_stream << "CASS_ERROR_LIB_NULL_VALUE";
      break;
    case CASS_ERROR_LIB_NOT_IMPLEMENTED:
      *output_stream << "CASS_ERROR_LIB_NOT_IMPLEMENTED";
      break;
    case CASS_ERROR_LIB_UNABLE_TO_CONNECT:
      *output_stream << "CASS_ERROR_LIB_UNABLE_TO_CONNECT";
      break;
    case CASS_ERROR_LIB_UNABLE_TO_CLOSE:
      *output_stream << "CASS_ERROR_LIB_UNABLE_TO_CLOSE";
      break;
    case CASS_ERROR_LIB_NO_PAGING_STATE:
      *output_stream << "CASS_ERROR_LIB_NO_PAGING_STATE";
      break;
    case CASS_ERROR_LIB_PARAMETER_UNSET:
      *output_stream << "CASS_ERROR_LIB_PARAMETER_UNSET";
      break;
    case CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE:
      *output_stream << "CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE";
      break;
    case CASS_ERROR_LIB_INVALID_FUTURE_TYPE:
      *output_stream << "CASS_ERROR_LIB_INVALID_FUTURE_TYPE";
      break;
    case CASS_ERROR_LIB_INTERNAL_ERROR:
      *output_stream << "CASS_ERROR_LIB_INTERNAL_ERROR";
      break;
    case CASS_ERROR_LIB_INVALID_CUSTOM_TYPE:
      *output_stream << "CASS_ERROR_LIB_INVALID_CUSTOM_TYPE";
      break;
    case CASS_ERROR_LIB_INVALID_DATA:
      *output_stream << "CASS_ERROR_LIB_INVALID_DATA";
      break;
    case CASS_ERROR_LIB_NOT_ENOUGH_DATA:
      *output_stream << "CASS_ERROR_LIB_NOT_ENOUGH_DATA";
      break;
    case CASS_ERROR_LIB_INVALID_STATE:
      *output_stream << "CASS_ERROR_LIB_INVALID_STATE";
      break;
    case CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID:
      *output_stream << "CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID";
      break;
    case CASS_ERROR_SERVER_SERVER_ERROR:
      *output_stream << "CASS_ERROR_SERVER_SERVER_ERROR";
      break;
    case CASS_ERROR_SERVER_PROTOCOL_ERROR:
      *output_stream << "CASS_ERROR_SERVER_PROTOCOL_ERROR";
      break;
    case CASS_ERROR_SERVER_BAD_CREDENTIALS:
      *output_stream << "CASS_ERROR_SERVER_BAD_CREDENTIALS";
      break;
    case CASS_ERROR_SERVER_UNAVAILABLE:
      *output_stream << "CASS_ERROR_SERVER_UNAVAILABLE";
      break;
    case CASS_ERROR_SERVER_OVERLOADED:
      *output_stream << "CASS_ERROR_SERVER_OVERLOADED";
      break;
    case CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
      *output_stream << "CASS_ERROR_SERVER_IS_BOOTSTRAPPING";
      break;
    case CASS_ERROR_SERVER_TRUNCATE_ERROR:
      *output_stream << "CASS_ERROR_SERVER_TRUNCATE_ERROR";
      break;
    case CASS_ERROR_SERVER_WRITE_TIMEOUT:
      *output_stream << "CASS_ERROR_SERVER_WRITE_TIMEOUT";
      break;
    case CASS_ERROR_SERVER_READ_TIMEOUT:
      *output_stream << "CASS_ERROR_SERVER_READ_TIMEOUT";
      break;
    case CASS_ERROR_SERVER_READ_FAILURE:
      *output_stream << "CASS_ERROR_SERVER_READ_FAILURE";
      break;
    case CASS_ERROR_SERVER_FUNCTION_FAILURE:
      *output_stream << "CASS_ERROR_SERVER_FUNCTION_FAILURE";
      break;
    case CASS_ERROR_SERVER_WRITE_FAILURE:
      *output_stream << "CASS_ERROR_SERVER_WRITE_FAILURE";
      break;
    case CASS_ERROR_SERVER_SYNTAX_ERROR:
      *output_stream << "CASS_ERROR_SERVER_SYNTAX_ERROR";
      break;
    case CASS_ERROR_SERVER_UNAUTHORIZED:
      *output_stream << "CASS_ERROR_SERVER_UNAUTHORIZED";
      break;
    case CASS_ERROR_SERVER_INVALID_QUERY:
      *output_stream << "CASS_ERROR_SERVER_INVALID_QUERY";
      break;
    case CASS_ERROR_SERVER_CONFIG_ERROR:
      *output_stream << "CASS_ERROR_SERVER_CONFIG_ERROR";
      break;
    case CASS_ERROR_SERVER_ALREADY_EXISTS:
      *output_stream << "CASS_ERROR_SERVER_ALREADY_EXISTS";
      break;
    case CASS_ERROR_SERVER_UNPREPARED:
      *output_stream << "CASS_ERROR_SERVER_UNPREPARED";
      break;
    case CASS_ERROR_SSL_INVALID_CERT:
      *output_stream << "CASS_ERROR_SSL_INVALID_CERT";
      break;
    case CASS_ERROR_SSL_INVALID_PRIVATE_KEY:
      *output_stream << "CASS_ERROR_SSL_INVALID_PRIVATE_KEY";
      break;
    case CASS_ERROR_SSL_NO_PEER_CERT:
      *output_stream << "CASS_ERROR_SSL_NO_PEER_CERT";
      break;
    case CASS_ERROR_SSL_INVALID_PEER_CERT:
      *output_stream << "CASS_ERROR_SSL_INVALID_PEER_CERT";
      break;
    case CASS_ERROR_SSL_IDENTITY_MISMATCH:
      *output_stream << "CASS_ERROR_SSL_IDENTITY_MISMATCH";
      break;
    case CASS_ERROR_SSL_PROTOCOL_ERROR:
      *output_stream << "CASS_ERROR_SSL_PROTOCOL_ERROR";
      break;
    default:
      *output_stream << "CASS ERROR NEEDS TO BE ADDED";
  }

  // Determine if the error description should be appended
  if (error_code != CASS_OK) {
    *output_stream << " [" << cass_error_desc(error_code) << "]";
  }
}

void PrintTo(CassValueType value_type, std::ostream* output_stream) {
  switch (value_type) {
    case CASS_VALUE_TYPE_CUSTOM:
      *output_stream << "CASS_VALUE_TYPE_CUSTOM";
      break;
    case CASS_VALUE_TYPE_ASCII:
      *output_stream << "CASS_VALUE_TYPE_ASCII";
      break;
    case CASS_VALUE_TYPE_BIGINT:
      *output_stream << "CASS_VALUE_TYPE_BIGINT";
      break;
    case CASS_VALUE_TYPE_BLOB:
      *output_stream << "CASS_VALUE_TYPE_BLOB";
      break;
    case CASS_VALUE_TYPE_BOOLEAN:
      *output_stream << "CASS_VALUE_TYPE_BOOLEAN";
      break;
    case CASS_VALUE_TYPE_COUNTER:
      *output_stream << "CASS_VALUE_TYPE_COUNTER";
      break;
    case CASS_VALUE_TYPE_DECIMAL:
      *output_stream << "CASS_VALUE_TYPE_DECIMAL";
      break;
    case CASS_VALUE_TYPE_DOUBLE:
      *output_stream << "CASS_VALUE_TYPE_DOUBLE";
      break;
    case CASS_VALUE_TYPE_FLOAT:
      *output_stream << "CASS_VALUE_TYPE_FLOAT";
      break;
    case CASS_VALUE_TYPE_INT:
      *output_stream << "CASS_VALUE_TYPE_INT";
      break;
    case CASS_VALUE_TYPE_TEXT:
      *output_stream << "CASS_VALUE_TYPE_TEXT";
      break;
    case CASS_VALUE_TYPE_TIMESTAMP:
      *output_stream << "CASS_VALUE_TYPE_TIMESTAMP";
      break;
    case CASS_VALUE_TYPE_UUID:
      *output_stream << "CASS_VALUE_TYPE_UUID";
      break;
    case CASS_VALUE_TYPE_VARCHAR:
      *output_stream << "CASS_VALUE_TYPE_VARCHAR";
      break;
    case CASS_VALUE_TYPE_VARINT:
      *output_stream << "CASS_VALUE_TYPE_VARINT";
      break;
    case CASS_VALUE_TYPE_TIMEUUID:
      *output_stream << "CASS_VALUE_TYPE_TIMEUUID";
      break;
    case CASS_VALUE_TYPE_INET:
      *output_stream << "CASS_VALUE_TYPE_INET";
      break;
    case CASS_VALUE_TYPE_DATE:
      *output_stream << "CASS_VALUE_TYPE_DATE";
      break;
    case CASS_VALUE_TYPE_TIME:
      *output_stream << "CASS_VALUE_TYPE_TIME";
      break;
    case CASS_VALUE_TYPE_SMALL_INT:
      *output_stream << "CASS_VALUE_TYPE_SMALL_INT";
      break;
    case CASS_VALUE_TYPE_TINY_INT:
      *output_stream << "CASS_VALUE_TYPE_TINY_INT";
      break;
    case CASS_VALUE_TYPE_DURATION:
      *output_stream << "CASS_VALUE_TYPE_DURATION";
      break;
    case CASS_VALUE_TYPE_LIST:
      *output_stream << "CASS_VALUE_TYPE_LIST";
      break;
    case CASS_VALUE_TYPE_MAP:
      *output_stream << "CASS_VALUE_TYPE_MAP";
      break;
    case CASS_VALUE_TYPE_SET:
      *output_stream << "CASS_VALUE_TYPE_SET";
      break;
    case CASS_VALUE_TYPE_UDT:
      *output_stream << "CASS_VALUE_TYPE_UDT";
      break;
    case CASS_VALUE_TYPE_TUPLE:
      *output_stream << "CASS_VALUE_TYPE_TUPLE";
      break;
    default:
      *output_stream << "CASS ERROR NEEDS TO BE ADDED";
  }
}
