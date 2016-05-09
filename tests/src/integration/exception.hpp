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
  const CassError error_code() const { return error_code_; }

private:
  /**
   * Error code associated with the exception
   */
  CassError error_code_;
};

} // namespace test

#endif // __TEST_EXCEPTION_HPP__
