/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "priming_result.hpp"

const PrimingResult PrimingResult::SUCCESS("success");
const PrimingResult PrimingResult::READ_REQUEST_TIMEOUT("request_timeout");
const PrimingResult PrimingResult::UNAVAILABLE("unavailable");
const PrimingResult PrimingResult::WRITE_REQUEST_TIMEOUT("write_request_timeout");
const PrimingResult PrimingResult::SERVER_ERROR("server_error");
const PrimingResult PrimingResult::PROTOCOL_ERROR("protocol_error");
const PrimingResult PrimingResult::BAD_CREDENTIALS("bad_credentials");
const PrimingResult PrimingResult::OVERLOADED("overloaded");
const PrimingResult PrimingResult::IS_BOOTSTRAPPING("is_bootstrapping");
const PrimingResult PrimingResult::TRUNCATE_ERROR("truncate_error");
const PrimingResult PrimingResult::SYNTAX_ERROR("syntax_error");
const PrimingResult PrimingResult::UNAUTHORIZED("unauthorized");
const PrimingResult PrimingResult::INVALID("invalid");
const PrimingResult PrimingResult::CONFIG_ERROR("config_error");
const PrimingResult PrimingResult::ALREADY_EXISTS("already_exists");
const PrimingResult PrimingResult::UNPREPARED("unprepared");
const PrimingResult PrimingResult::CLOSED_CONNECTION("closed_connection");

PrimingResult::PrimingResult(const std::string& json_value)
  : json_value_(json_value) {}

const std::string& PrimingResult::json_value() const {
  return json_value_;
}
