/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __PRIMING_RESULT_HPP__
#define __PRIMING_RESULT_HPP__

#include <string>

/**
 * Priming result request response
 */
class PrimingResult {
public:

#define MAKE_PRIMING_RESULT(FUNC) \
  static const PrimingResult FUNC() { \
    return PrimingResult(#FUNC); \
  }

  /**
   * Success
   */
  MAKE_PRIMING_RESULT(success);
  /**
   * Read request timeout
   */
  MAKE_PRIMING_RESULT(request_timeout);
  /**
   * Unavailable
   */
  MAKE_PRIMING_RESULT(unavailable);
  /**
   * Write request timeout
   */
  MAKE_PRIMING_RESULT(write_request_timeout);
  /**
   * Server error
   */
  MAKE_PRIMING_RESULT(server_error);
  /**
   * Protocol error
   */
  MAKE_PRIMING_RESULT(protocol_error);
  /**
   * Bas/Invalid credentials
   */
  MAKE_PRIMING_RESULT(bad_credentials);
  /**
   * Overloaded
   */
  MAKE_PRIMING_RESULT(overloaded);
  /**
   * Bootstrapping
   */
  MAKE_PRIMING_RESULT(is_bootstrapping);
  /**
   * Truncate error
   */
  MAKE_PRIMING_RESULT(truncate_error);
  /**
   * Syntax error
   */
  MAKE_PRIMING_RESULT(syntax_error);
  /**
   * Unauthorized
   */
  MAKE_PRIMING_RESULT(unauthorized);
  /**
   * Invalid
   */
  MAKE_PRIMING_RESULT(invalid);
  /**
   * Configuration error
   */
  MAKE_PRIMING_RESULT(config_error);
  /**
   * Already exists
   */
  MAKE_PRIMING_RESULT(already_exists);
  /**
   * Unprepared
   */
  MAKE_PRIMING_RESULT(unprepared);
  /**
   * Closed connection
   */
  MAKE_PRIMING_RESULT(closed_connection);

#undef MAKE_PRIMING_RESULT

  inline const std::string& json_value() const {
    return json_value_;
  }

private:
  /**
   * JSON value associated with the result constant
   */
  std::string json_value_;

  /**
   * Create the result constant for priming the result of an executed query
   *
   * @param json_value JSON value to associated with the constant
   */
  PrimingResult(const std::string& json_value) : json_value_(json_value) {}
};

#endif //__PRIMING_RESULT_HPP__
