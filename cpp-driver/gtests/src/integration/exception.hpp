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
  CassException(const std::string& message, const CassError error_code)
      : Exception(message)
      , error_code_(error_code) {}

  /**
   * Exception class that contains an error code
   *
   * @param message Exception message
   * @param error_code Error code
   * @param error_code Error message
   */
  CassException(const std::string& message, const CassError error_code,
                const std::string& error_message)
      : Exception(message)
      , error_code_(error_code)
      , error_message_(error_message) {}

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

  /**
   * Get the human readable description of the error code
   *
   * @return Error description
   */
  const std::string error_description() { return std::string(cass_error_desc(error_code())); }

  /**
   * Get the error message associated with the exception
   *
   * @return Error message
   */
  const std::string error_message() { return error_message_; }

private:
  /**
   * Error code associated with the exception
   */
  CassError error_code_;
  /**
   * Error message associated with the exception
   */
  std::string error_message_;
};

} // namespace test

#endif // __TEST_EXCEPTION_HPP__
