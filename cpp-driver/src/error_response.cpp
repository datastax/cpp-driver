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

#include "error_response.hpp"

#include "external.hpp"
#include "logger.hpp"
#include "serialization.hpp"

#include <iomanip>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

extern "C" {

void cass_error_result_free(const CassErrorResult* error_result) { error_result->dec_ref(); }

CassError cass_error_result_code(const CassErrorResult* error_result) {
  return static_cast<CassError>(CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error_result->code()));
}

CassConsistency cass_error_result_consistency(const CassErrorResult* error_result) {
  return error_result->consistency();
}

cass_int32_t cass_error_result_responses_received(const CassErrorResult* error_result) {
  return error_result->received();
}

cass_int32_t cass_error_result_responses_required(const CassErrorResult* error_result) {
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

CassError cass_error_result_keyspace(const CassErrorResult* error_result, const char** keyspace,
                                     size_t* keyspace_length) {
  if (error_result->code() != CQL_ERROR_ALREADY_EXISTS &&
      error_result->code() != CQL_ERROR_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *keyspace = error_result->keyspace().data();
  *keyspace_length = error_result->keyspace().size();
  return CASS_OK;
}

CassError cass_error_result_table(const CassErrorResult* error_result, const char** table,
                                  size_t* table_length) {
  if (error_result->code() != CQL_ERROR_ALREADY_EXISTS) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *table = error_result->table().data();
  *table_length = error_result->table().size();
  return CASS_OK;
}

CassError cass_error_result_function(const CassErrorResult* error_result, const char** function,
                                     size_t* function_length) {
  if (error_result->code() != CQL_ERROR_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  *function = error_result->function().data();
  *function_length = error_result->function().size();
  return CASS_OK;
}

size_t cass_error_num_arg_types(const CassErrorResult* error_result) {
  return error_result->arg_types().size();
}

CassError cass_error_result_arg_type(const CassErrorResult* error_result, size_t index,
                                     const char** arg_type, size_t* arg_type_length) {
  if (error_result->code() != CQL_ERROR_FUNCTION_FAILURE) {
    return CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE;
  }
  if (index > error_result->arg_types().size()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  StringRef arg_type_ref = error_result->arg_types()[index];
  *arg_type = arg_type_ref.data();
  *arg_type_length = arg_type_ref.size();
  return CASS_OK;
}

} // extern "C"

String ErrorResponse::error_message() const {
  OStringStream ss;
  ss << "'" << message().to_string() << "'"
     << " (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
     << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, code()) << ")";
  return ss.str();
}

bool ErrorResponse::decode(Decoder& decoder) {
  decoder.set_type("error");
  CHECK_RESULT(decoder.decode_int32(code_));
  CHECK_RESULT(decoder.decode_string(&message_));

  switch (code_) {
    case CQL_ERROR_UNAVAILABLE:
      CHECK_RESULT(decoder.decode_uint16(cl_));
      CHECK_RESULT(decoder.decode_int32(required_));
      CHECK_RESULT(decoder.decode_int32(received_));
      break;
    case CQL_ERROR_READ_TIMEOUT:
      CHECK_RESULT(decoder.decode_uint16(cl_));
      CHECK_RESULT(decoder.decode_int32(received_));
      CHECK_RESULT(decoder.decode_int32(required_));
      CHECK_RESULT(decoder.decode_byte(data_present_));
      break;
    case CQL_ERROR_WRITE_TIMEOUT:
      CHECK_RESULT(decoder.decode_uint16(cl_));
      CHECK_RESULT(decoder.decode_int32(received_));
      CHECK_RESULT(decoder.decode_int32(required_));
      CHECK_RESULT(decoder.decode_write_type(write_type_));
      break;
    case CQL_ERROR_READ_FAILURE:
      CHECK_RESULT(decoder.decode_uint16(cl_));
      CHECK_RESULT(decoder.decode_int32(received_));
      CHECK_RESULT(decoder.decode_int32(required_));
      CHECK_RESULT(decoder.decode_failures(failures_, num_failures_));
      CHECK_RESULT(decoder.decode_byte(data_present_));
      break;
    case CQL_ERROR_FUNCTION_FAILURE:
      CHECK_RESULT(decoder.decode_string(&keyspace_));
      CHECK_RESULT(decoder.decode_string(&function_));
      CHECK_RESULT(decoder.decode_stringlist(arg_types_));
      break;
    case CQL_ERROR_WRITE_FAILURE:
      CHECK_RESULT(decoder.decode_uint16(cl_));
      CHECK_RESULT(decoder.decode_int32(received_));
      CHECK_RESULT(decoder.decode_int32(required_));
      CHECK_RESULT(decoder.decode_failures(failures_, num_failures_));
      CHECK_RESULT(decoder.decode_write_type(write_type_));
      break;
    case CQL_ERROR_UNPREPARED:
      CHECK_RESULT(decoder.decode_string(&prepared_id_));
      break;
    case CQL_ERROR_ALREADY_EXISTS:
      CHECK_RESULT(decoder.decode_string(&keyspace_));
      CHECK_RESULT(decoder.decode_string(&table_));
      break;
  }

  decoder.maybe_log_remaining();
  return true;
}

bool check_error_or_invalid_response(const String& prefix, uint8_t expected_opcode,
                                     const Response* response) {
  if (response->opcode() == expected_opcode) {
    return false;
  }

  OStringStream ss;
  if (response->opcode() == CQL_OPCODE_ERROR) {
    ss << prefix << ": Error response "
       << static_cast<const ErrorResponse*>(response)->error_message();
  } else {
    ss << prefix << ": Unexpected opcode " << opcode_to_string(response->opcode());
  }

  LOG_ERROR("%s", ss.str().c_str());

  return true;
}
