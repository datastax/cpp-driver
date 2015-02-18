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

bool ErrorResponse::decode(int version, char* buffer, size_t size) {
  char* pos = decode_int32(buffer, code_);
  pos = decode_string(pos, &message_, message_size_);
  switch (code_) {
    case CQL_ERROR_UNPREPARED:
      decode_string(pos, &prepared_id_, prepared_id_size_);
      break;
  }
  return true;
}

std::string error_response_message(const std::string& prefix, ErrorResponse* error) {
  std::ostringstream ss;
  ss << prefix << ": '" << error->message()
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
     << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error->code()) << ")";
  return ss.str();
}

bool check_error_or_invalid_response(const std::string& prefix, uint8_t expected_opcode,
                                     Response* response) {
  if (response->opcode() == expected_opcode) {
    return false;
  }

  if (response->opcode() == CQL_OPCODE_ERROR) {
    std::ostringstream ss;
    ss << prefix << ": Error response";
    LOG_ERROR("%s", error_response_message(ss.str(), static_cast<ErrorResponse*>(response)).c_str());
  } else {
    std::ostringstream ss;
    ss << prefix << ": Unexpected opcode " << opcode_to_string(response->opcode());
    LOG_ERROR("%s", ss.str().c_str());
  }

  return true;
}

} // namespace cass
