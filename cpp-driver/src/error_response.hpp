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

#ifndef DATASTAX_INTERNAL_ERROR_RESPONSE_HPP
#define DATASTAX_INTERNAL_ERROR_RESPONSE_HPP

#include "constants.hpp"
#include "external.hpp"
#include "response.hpp"
#include "retry_policy.hpp"
#include "scoped_ptr.hpp"
#include "string.hpp"
#include "string_ref.hpp"
#include "vector.hpp"

#include <uv.h>

#include <string.h>

namespace datastax { namespace internal { namespace core {

class ErrorResponse : public Response {
public:
  ErrorResponse()
      : Response(CQL_OPCODE_ERROR)
      , code_(-1)
      , cl_(CASS_CONSISTENCY_UNKNOWN)
      , received_(-1)
      , required_(-1)
      , num_failures_(-1)
      , data_present_(0)
      , write_type_(CASS_WRITE_TYPE_UNKNOWN) {}

  ErrorResponse(int32_t code, StringRef message)
      : Response(CQL_OPCODE_ERROR)
      , code_(code)
      , message_(message) {}

  int32_t code() const { return code_; }
  StringRef message() const { return message_; }
  StringRef prepared_id() const { return prepared_id_; }
  CassConsistency consistency() const { return static_cast<CassConsistency>(cl_); }
  int32_t received() const { return received_; }
  int32_t required() const { return required_; }
  int32_t num_failures() const { return num_failures_; }
  uint8_t data_present() const { return data_present_; }
  CassWriteType write_type() const { return write_type_; }
  StringRef keyspace() const { return keyspace_; }
  StringRef table() const { return table_; }
  StringRef function() const { return function_; }
  const StringRefVec& arg_types() const { return arg_types_; }

  String error_message() const;

  virtual bool decode(Decoder& decoder);

private:
  int32_t code_;
  StringRef message_;
  StringRef prepared_id_;
  uint16_t cl_;
  int32_t received_;
  int32_t required_;
  int32_t num_failures_;
  FailureVec failures_;
  uint8_t data_present_;
  CassWriteType write_type_;
  StringRef keyspace_;
  StringRef table_;
  StringRef function_;
  StringRefVec arg_types_;
};

bool check_error_or_invalid_response(const String& prefix, uint8_t expected_opcode,
                                     const Response* response);

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::ErrorResponse, CassErrorResult)

#endif
