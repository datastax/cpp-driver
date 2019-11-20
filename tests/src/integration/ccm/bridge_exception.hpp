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

#ifndef __CCM_BRIDGE_EXCEPTION_HPP__
#define __CCM_BRIDGE_EXCEPTION_HPP__

#include <exception>
#include <string>

namespace CCM {

/**
 * Exception mechanism for the CCM bridge class
 */
class BridgeException : public std::exception {
public:
#ifdef _WIN32
  /**
   * CCM bridge exception class
   *
   * @param message BridgeException message
   */
  BridgeException(const std::string& message)
      : std::exception(message.c_str()) {}
#else
  /**
   * CCM bridge exception class
   *
   * @param message Exception message
   */
  BridgeException(const std::string& message)
      : message_(message) {}
  /**
   * Destructor
   */
  ~BridgeException() throw() {}

  /**
   * Get the exception message
   *
   * @return Exception message
   */
  const char* what() const throw() { return message_.c_str(); }

private:
  /**
   * Message to display for exception
   */
  std::string message_;
#endif
};

} // namespace CCM

#endif // __CCM_BRIDGE_EXCEPTION_HPP__
