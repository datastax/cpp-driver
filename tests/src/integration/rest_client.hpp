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

#ifndef __TEST_REST_CLIENT_HPP__
#define __TEST_REST_CLIENT_HPP__

#include "exception.hpp"

#include <string>

#include <uv.h>

/**
 * REST request
 */
struct Request {
  /**
   * HTTP request method
   */
  enum Method { HTTP_METHOD_DELETE = 0, HTTP_METHOD_GET, HTTP_METHOD_POST };

  /**
   * Host address IPv4
   */
  std::string address;
  /**
   * Host port
   */
  unsigned short port;
  /**
   * JSON message to put in the POST request content
   */
  std::string content;
  /**
   * The endpoint (URI) for the request
   */
  std::string endpoint;
  /**
   * HTTP request type to use for the request
   */
  Method method;
};

/**
 * REST response
 */
struct Response {
  /**
   * JSON result message
   */
  std::string message;
  /**
   * Status code/response for the HTTP request
   */
  int status_code;

  Response()
      : status_code(200 /* OK */) {}
};

/**
 * Simple HTTP client for sending synchronous requests to a HTTP REST server
 * REST server
 */
class RestClient {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  /**
   * Send/Submit the request to the REST server
   *
   * @param request The request to send
   * @return The response from the request
   */
  static const Response send_request(const Request& request);

private:
  /**
   * libuv write buffer
   */
  static uv_buf_t write_buf_;
  /**
   * libuv write request handle
   */
  static uv_write_t write_req_;
  /**
   * Disable the default constructor
   */
  RestClient();

  /**
   * Handle the buffer allocation of memory for the requests and server
   * responses.
   *
   * @param handle The libuv handle type
   * @param suggested_size The size (in bytes) to allocate for the buffer
   * @param buf Buffer to be allocated
   */
  static void handle_allocation(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buffer);

  /**
   * Handle the connect (callback) when the connection has been established to
   * the REST server.
   *
   * @param req Connection request type
   * @param status 0 if connection was successful; < 0 otherwise
   */
  static void handle_connected(uv_connect_t* req, int status);

  /**
   * Handle the response (callback) of the data coming from the stream/socket.
   *
   * @param stream Stream handle type
   * @param nread > 0 if data is available (can be read); < 0 otherwise.
   *              NOTE: When there is no more data in the stream <b>nread</b>
   *              will equal <b>UV_EOF</b>
   * @param buf Buffer to read from
   */
  static void handle_response(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf);

  /**
   * Generate the HTTP message for the REST request.
   *
   * @param request Request to generate message from
   * @return String representing the REST request HTTP message
   */
  static const std::string generate_http_message(const Request& request);
};

#endif // __TEST_REST_CLIENT_HPP__
