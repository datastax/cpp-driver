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

#ifndef __TSOCKET_HPP__
#define __TSOCKET_HPP__

#ifdef _WIN32
#include <winsock2.h>
#define SOCKET_HANDLE SOCKET
#else
#define SOCKET_HANDLE int
#endif

#include "socket_exception.hpp"

#define DEFAULT_SOCKET_TIMEOUT_IN_SECONDS 10

class Socket {
public:
  /**
   * Constructor
   *
   * @param timeout Timeout to apply for socket operations
   *                (default: DEFAULT_SOCKET_TIMEOUT_IN_SECONDS)
   * @throws SocketException if unable to initialize socket
   */
  Socket(long timeout = DEFAULT_SOCKET_TIMEOUT_IN_SECONDS);
  /**
   * Destructor
   */
  ~Socket();
  /**
   * Establish the socket connection
   *
   * @param ip_address IP address to connect to
   * @param port Port number to establish connection on
   */
  void establish_connection(const std::string& ip_address, unsigned short port);
  /**
   * Get the socket handle
   *
   * @return Socket handle
   */
  SOCKET_HANDLE get_handle();
  /**
   * Synchronize the socket
   */
  void synchronize(bool is_read, bool is_write);

private:
  /**
   * Timeout to apply for socket operations
   */
  long timeout_;
  /**
   * Socket handle
   */
  SOCKET_HANDLE handle_;

#ifdef _WIN32
  /**
   * Initialize the socket component
   *
   * @throws SocketException if unable to initialize socket
   */
  void initialize_sockets();
#endif
  /**
   * Get error message from the error code
   *
   * @param error_code Error code to retrieve error message
   * @return Error message
   */
  std::string get_error_message(int error_code) const;
};

#endif // __TSOCKET_HPP__
