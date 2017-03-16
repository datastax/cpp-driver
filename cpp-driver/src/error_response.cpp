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

#include "error_response.hpp"

#include "external.hpp"
#include "logger.hpp"
#include "serialization.hpp"

#include <iomanip>
#include <sstream>

extern "C" {

void cass_error_result_free(const CassErrorResult* error_result) {
  error_result->dec_ref();
}

CassError cass_error_result_code(const CassErrorResult* error_result) {
  return static_cast<CassError>(
        CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error_result->code()));
}

CassConsistency cass_error_result_consistency(const CassErrorResult* error_result) {
  return error_result->consistency();
}

cass_int32_t cass_error_result_actual(const CassErrorResult* error_result) {
  return error_result->received();
}

cass_int32_t cass_error_result_required(const CassErrorResult* error_result) {
  return error_result->required();
}

cass_int32_t cass_error_result_num_failures(const CassErrorResult* error_result) {
  return error_result->num_failures();
}

cass_bool_t cass_error_result_data_present(const CassErrorResult* error_result) {
  return static_cast<cass_bool_t>(error_result->data_present());
}

CassWriteType cass_error_result_write_type(const CassErrorResult* error_result) {
  return error_result->write_type();
}

CassError cass_error_result_keyspace(const CassErrorResult* error_result,
                                     const char** keyspace,
                                     size_t* keyspace_length) {
  if (error_result->code() != CASS_ERROR_SERVER_ALREADY_EXISTS ||
      error_result->code() != CASS_ERROR_SERVER_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *keyspace = error_result->keyspace().data();
  *keyspace_length = error_result->keyspace().size();
  return CASS_OK;
}

CassError cass_error_result_table(const CassErrorResult* error_result,
                                  const char** table,
                                  size_t* table_length) {
  if (error_result->code() != CASS_ERROR_SERVER_ALREADY_EXISTS) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *table = error_result->table().data();
  *table_length = error_result->table().size();
  return CASS_OK;
}

CassError cass_error_result_function(const CassErrorResult* error_result,
                                     const char** function,
                                     size_t* function_length) {
  if (error_result->code() != CASS_ERROR_SERVER_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *function = error_result->function().data();
  *function_length = error_result->function().size();
  return CASS_OK;
}

size_t cass_error_num_arg_types(const CassErrorResult* error_result) {
  return error_result->arg_types().size();
}

CassError cass_error_result_arg_type(const CassErrorResult* error_result,
                                     size_t index,
                                     const char** arg_type,
                                     size_t* arg_type_length) {
  if (error_result->code() != CASS_ERROR_SERVER_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  if (index > error_result->arg_types().size()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  cass::StringRef arg_type_ref = error_result->arg_types()[index];
  *arg_type = arg_type_ref.data();
  *arg_type_length = arg_type_ref.size();
  return CASS_OK;
}

} // extern "C"

namespace cass {

std::string ErrorResponse::error_message() const {
  std::ostringstream ss;
  ss << "'" << message().to_string() << "'"
     << " (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
     << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, code()) << ")";
  return ss.str();
}

bool ErrorResponse::decode(int version, char* buffer, size_t size) {
  char* pos = decode_int32(buffer, code_);
  pos = decode_string(pos, &message_);

  switch (code_) {
    case CQL_ERROR_UNAVAILABLE:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, required_);
      decode_int32(pos, received_);
      break;
    case CQL_ERROR_READ_TIMEOUT:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      decode_byte(pos, data_present_);
      break;
    case CQL_ERROR_WRITE_TIMEOUT:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      decode_write_type(pos);
      break;
    case CQL_ERROR_READ_FAILURE:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      pos = decode_int32(pos, num_failures_);
      if (version >= 5) {
        pos = decode_failures(pos);
      }
      decode_byte(pos, data_present_);
      break;
    case CQL_ERROR_FUNCTION_FAILURE:
      pos = decode_string(pos, &keyspace_);
      pos = decode_string(pos, &function_);
      decode_stringlist(pos, arg_types_);
      break;
    case CQL_ERROR_WRITE_FAILURE:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      pos = decode_int32(pos, num_failures_);
      if (version >= 5) {
        pos = decode_failures(pos);
      }
      decode_write_type(pos);
      break;
    case CQL_ERROR_UNPREPARED:
      decode_string(pos, &prepared_id_);
      break;
    case CQL_ERROR_ALREADY_EXISTS:
      pos = decode_string(pos, &keyspace_);
      pos = decode_string(pos, &table_);
      break;
  }
  return true;
}

// Format: <endpoint><failurecode>
// where:
// <endpoint> is a [inetaddr]
// <failurecode> is a [short]
char* ErrorResponse::decode_failures(char* pos) {
  failures_.reserve(num_failures_);
  for (int32_t i = 0; i < num_failures_; ++i) {
    Failure failure;
    pos = decode_inet(pos, &failure.endpoint);
    pos = decode_uint16(pos, failure.failurecode);
    failures_.push_back(failure);
  }
  return pos;
}

void ErrorResponse::decode_write_type(char* pos) {
  StringRef write_type;
  decode_string(pos, &write_type);
  if (write_type == "SIMPLE") {
    write_type_ = CASS_WRITE_TYPE_SIMPLE;
  } else if(write_type == "BATCH") {
    write_type_ = CASS_WRITE_TYPE_BATCH;
  } else if(write_type == "UNLOGGED_BATCH") {
    write_type_ = CASS_WRITE_TYPE_UNLOGGED_BATCH;
  } else if(write_type == "COUNTER") {
    write_type_ = CASS_WRITE_TYPE_COUNTER;
  } else if(write_type == "BATCH_LOG") {
    write_type_ = CASS_WRITE_TYPE_BATCH_LOG;
  }
}

bool check_error_or_invalid_response(const std::string& prefix, uint8_t expected_opcode,
                                     Response* response) {
  if (response->opcode() == expected_opcode) {
    return false;
  }

  std::ostringstream ss;
  if (response->opcode() == CQL_OPCODE_ERROR) {
    ss << prefix << ": Error response "
       << static_cast<ErrorResponse*>(response)->error_message();
  } else {
    ss << prefix << ": Unexpected opcode "
       << opcode_to_string(response->opcode());
  }

  LOG_ERROR("%s", ss.str().c_str());

  return true;
}

} // namespace cass
