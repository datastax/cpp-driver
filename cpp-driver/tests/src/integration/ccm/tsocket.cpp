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

#include "tsocket.hpp"

#include <sstream>

#ifndef _WIN32
#include <arpa/inet.h>
#include <cstring>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#endif

Socket::Socket(long timeout /*= SOCKET_TIMEOUT_IN_SECONDS*/)
    : timeout_(timeout) {
#ifdef _WIN32
  initialize_sockets();
#endif
}

Socket::~Socket() {
#ifdef _WIN32
  closesocket(handle_);
  WSACleanup();
#else
  close(handle_);
#endif
}

SOCKET_HANDLE Socket::get_handle() { return handle_; }

#ifdef _WIN32
void Socket::initialize_sockets() {
  WSADATA wsa_data;
  int error_code = WSAStartup(MAKEWORD(2, 0), &wsa_data);
  if (error_code != 0) {
    std::string message = get_error_message(error_code);
    throw SocketException("Unable to initialize Windows sockets: " + message);
  }
}
#endif

std::string Socket::get_error_message(int error_code) const {
  // Format the error code message using the default system language
  std::stringstream message;
#ifdef _WIN32
  LPVOID error_string;
  int size =
      FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL, error_code,
                    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)&error_string, 0, NULL);

  // Ensure the message could be retrieved and update the return message
  if (size) {
    message << std::string((LPTSTR)error_string, size);
    LocalFree(error_string);
  }
#else
  message << std::string(strerror(error_code));
#endif
  message << " [" << error_code << "]";
  return message.str();
}

void Socket::synchronize(bool is_read, bool is_write) {
  fd_set socket;
  FD_ZERO(&socket);
  FD_SET(handle_, &socket);

  // Determine current read/write direction of the session
  fd_set* read = NULL;
  fd_set* write = NULL;
  if (is_read) {
    read = &socket;
  }
  if (is_write) {
    write = &socket;
  }

  // Get the status of the socket (synchronize/wait on socket)
  struct timeval timeout;
  timeout.tv_sec = timeout_;
  timeout.tv_usec = 0;
  if (select(static_cast<int>(handle_) + 1, read, write, NULL, &timeout) == SOCKET_ERROR) {
    std::string message = "Failed to Synchronize Socket: ";
#ifdef _WIN32
    message += get_error_message(GetLastError());
#else
    message += get_error_message(errno);
#endif
    throw SocketException(message);
  }
}

void Socket::establish_connection(const std::string& ip_address, unsigned short port) {
  handle_ = socket(AF_INET, SOCK_STREAM, 0);
  if (handle_ == INVALID_SOCKET) {
    std::string message = "Failed to Create Socket: ";
#ifdef _WIN32
    message += get_error_message(GetLastError());
#else
    message += get_error_message(errno);
#endif
    throw SocketException(message);
  }

  struct sockaddr_in ipv4_socket_address;
  ipv4_socket_address.sin_family = AF_INET;
  ipv4_socket_address.sin_port = htons(port);
  ipv4_socket_address.sin_addr.s_addr = inet_addr(ip_address.c_str());
  if (connect(handle_, (struct sockaddr*)(&ipv4_socket_address), sizeof(struct sockaddr_in)) != 0) {
    std::string message = "Failed to Establish Connection: ";
#ifdef _WIN32
    message += get_error_message(GetLastError());
#else
    message += get_error_message(errno);
#endif
    throw SocketException(message);
  }
}
