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

#ifndef __SOCKET_EXCEPTION_HPP__
#define __SOCKET_EXCEPTION_HPP__

#include <exception>
#include <string>

/**
 * Exception mechanism for the socket class
 */
class SocketException : public std::exception {
public:
#ifdef _WIN32
  /**
   * Socket exception class
   *
   * @param message SocketException message
   */
  SocketException(const std::string& message)
      : std::exception(message.c_str()) {}
#else
  /**
   * Socket exception class
   *
   * @param message Exception message
   */
  SocketException(const std::string& message)
      : message_(message) {}
  /**
   * Destructor
   */
  ~SocketException() throw() {}

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

#endif // __SOCKET_EXCEPTION_HPP__
