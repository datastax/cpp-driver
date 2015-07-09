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

#include "error_response.hpp"

#include "logger.hpp"
#include "serialization.hpp"

#include <iomanip>
#include <sstream>

namespace cass {

std::string ErrorResponse::error_message() const {
  std::ostringstream ss;
  ss << "'" << message().to_string() << "'"
     << " (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
     << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, code()) << ")";
  return ss.str();
}

bool ErrorResponse::decode(int version, char* buffer, size_t size) {
  uint8_t data_present;
  StringRef write_type;

  char* pos = decode_int32(buffer, code_);
  pos = decode_string_ref(pos, &message_);

  switch (code_) {
    case CQL_ERROR_UNAVAILABLE:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, alive_);
      break;
    case CQL_ERROR_READ_TIMEOUT:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      decode_byte(pos, data_present);
      data_present_ = data_present > 0;
      break;
    case CQL_ERROR_WRITE_TIMEOUT:
      pos = decode_uint16(pos, cl_);
      pos = decode_int32(pos, received_);
      pos = decode_int32(pos, required_);
      decode_string_ref(pos, &write_type);
      if (write_type == "SIMPLE") {
        write_type_ = RetryPolicy::SIMPLE;
      } else if(write_type == "BATCH") {
        write_type_ = RetryPolicy::BATCH;
      } else if(write_type == "UNLOGGED_BATCH") {
        write_type_ = RetryPolicy::UNLOGGED_BATCH;
      } else if(write_type == "COUNTER") {
        write_type_ = RetryPolicy::COUNTER;
      } else if(write_type == "BATCH_LOG") {
        write_type_ = RetryPolicy::BATCH_LOG;
      }
      break;
    case CQL_ERROR_UNPREPARED:
      decode_string_ref(pos, &prepared_id_);
      break;
  }
  return true;
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
