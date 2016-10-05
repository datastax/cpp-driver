/*
  Copyright (c) 2016 DataStax, Inc.

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
  /**
   * Success
   */
  static const PrimingResult SUCCESS;
  /**
   * Read request timeout
   */
  static const PrimingResult READ_REQUEST_TIMEOUT;
  /**
   * Unavailable
   */
  static const PrimingResult UNAVAILABLE;
  /**
   * Write request timeout
   */
  static const PrimingResult WRITE_REQUEST_TIMEOUT;
  /**
   * Server error
   */
  static const PrimingResult SERVER_ERROR;
  /**
   * Protocol error
   */
  static const PrimingResult PROTOCOL_ERROR;
  /**
   * Bas/Invalid credentials
   */
  static const PrimingResult BAD_CREDENTIALS;
  /**
   * Overloaded
   */
  static const PrimingResult OVERLOADED;
  /**
   * Bootstrapping
   */
  static const PrimingResult IS_BOOTSTRAPPING;
  /**
   * Truncate error
   */
  static const PrimingResult TRUNCATE_ERROR;
  /**
   * Syntax error
   */
  static const PrimingResult SYNTAX_ERROR;
  /**
   * Unauthorized
   */
  static const PrimingResult UNAUTHORIZED;
  /**
   * Invalid
   */
  static const PrimingResult INVALID;
  /**
   * Configuration error
   */
  static const PrimingResult CONFIG_ERROR;
  /**
   * Already exists
   */
  static const PrimingResult ALREADY_EXISTS;
  /**
   * Unprepared
   */
  static const PrimingResult UNPREPARED;
  /**
   * Closed connection
   */
  static const PrimingResult CLOSED_CONNECTION;

  const std::string& json_value() const;

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
  PrimingResult(const std::string& json_value);
};

#endif //__PRIMING_RESULT_HPP__
