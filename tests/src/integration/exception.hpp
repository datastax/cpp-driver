/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_EXCEPTION_HPP__
#define __TEST_EXCEPTION_HPP__
#include <stdexcept>

#include "cassandra.h"

#include <string>

namespace test {

/**
 * Base exception class
 */
class Exception : public std::runtime_error {
public:
  /**
   * Base exception class
   *
   * @param message Exception message
   */
  Exception(const std::string& message)
    : std::runtime_error(message) {}

  /**
   * Destructor
   */
  virtual ~Exception() throw() {}
};

/**
 * Exception mechanism for test classes
 */
class CassException : public Exception {
public:
  /**
   * Exception class that contains an error code
   *
   * @param message Exception message
   * @param error_code Error code
   */
  CassException(const std::string &message, const CassError error_code)
    : Exception(message)
    , error_code_(error_code) {}

  /**
   * Destructor
   */
  virtual ~CassException() throw() {}

  /**
   * Get the error code associated with the exception
   *
   * @return Error code
   */
  CassError error_code() const { return error_code_; }

private:
  /**
   * Error code associated with the exception
   */
  CassError error_code_;
};

} // namespace test

#endif // __TEST_EXCEPTION_HPP__
