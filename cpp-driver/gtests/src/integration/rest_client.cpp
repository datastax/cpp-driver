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

#include "rest_client.hpp"
#include "test_utils.hpp"
#include "tlog.hpp"

#include "address.hpp"

#include <sstream>

#define HTTP_EOL "\r\n"
#define OUTPUT_BUFFER_SIZE 10240ul

using namespace datastax::internal::core;

// Static initializations
uv_buf_t RestClient::write_buf_;
uv_write_t RestClient::write_req_;

/**
+ * HTTP request
+ */
struct HttpRequest {
  /**
   * HTTP message to submit to the REST server
   */
  std::string message;
  /**
   * libuv loop (for processing errors)
   */
  uv_loop_t* loop;
  /**
   * HTTP response for sent request
   */
  Response response;
};

const Response RestClient::send_request(const Request& request) {
  // Initialize the loop
  uv_loop_t loop;
  int error_code = uv_loop_init(&loop);
  if (error_code != 0) {
    throw Exception("Unable to Send Request: " + std::string(uv_strerror(error_code)));
  };

  // Initialize the HTTP request
  HttpRequest http_request;
  http_request.message = generate_http_message(request);
  http_request.loop = &loop;

  // Create the IPv4 socket address
  const Address address(request.address.c_str(), static_cast<int>(request.port));

  // Initialize the client TCP request
  uv_tcp_t tcp;
  tcp.data = &http_request;
  error_code = uv_tcp_init(&loop, &tcp);
  if (error_code != 0) {
    TEST_LOG_ERROR("Unable to Initialize TCP Request: " + std::string(uv_strerror(error_code)));
  }
  error_code = uv_tcp_keepalive(&tcp, 1, 60);
  if (error_code != 0) {
    TEST_LOG_ERROR("Unable to Set TCP KeepAlive: " + std::string(uv_strerror(error_code)));
  }

  // Start the request and attach the HTTP request to send to the REST server
  uv_connect_t connect;
  connect.data = &http_request;
  Address::SocketStorage storage;
  uv_tcp_connect(&connect, &tcp, address.to_sockaddr(&storage), handle_connected);

  uv_run(&loop, UV_RUN_DEFAULT);
  uv_loop_close(&loop);

  // Return the response from the request
  return http_request.response;
}

void RestClient::handle_allocation(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buffer) {
  buffer->base = new char[OUTPUT_BUFFER_SIZE];
  buffer->len = OUTPUT_BUFFER_SIZE;
}

void RestClient::handle_connected(uv_connect_t* req, int status) {
  HttpRequest* request = static_cast<HttpRequest*>(req->data);

  if (status < 0) {
    TEST_LOG_ERROR("Unable to Connect to HTTP Server: " << uv_strerror(status));
    uv_close(reinterpret_cast<uv_handle_t*>(req->handle), NULL);
  } else {
    // Create the buffer to write to the stream
    write_buf_.base = const_cast<char*>(request->message.c_str());
    write_buf_.len = request->message.size();

    // Send the HTTP request
    uv_read_start(req->handle, handle_allocation, handle_response);
    uv_write(&write_req_, req->handle, &write_buf_, 1, NULL);
  }
}

void RestClient::handle_response(uv_stream_t* stream, ssize_t buffer_length,
                                 const uv_buf_t* buffer) {
  HttpRequest* request = static_cast<HttpRequest*>(stream->data);

  if (buffer_length > 0) {
    // Process the buffer and log it
    std::string server_response = std::string(buffer->base, buffer_length);
    TEST_LOG(test::Utils::trim(server_response));

    // Parse the status code and content of the response
    std::istringstream lines(server_response);
    std::string current_line;
    while (std::getline(lines, current_line)) {
      // Determine if the status code should be assigned in the response
      if (current_line.compare(0, 4, "HTTP") == 0) {
        // Status-Line = HTTP-Version <SPC> Status-Code <SPC> Reason-Phrase
        std::stringstream status_code;
        status_code << test::Utils::explode(current_line, ' ').at(1);
        if ((status_code >> request->response.status_code).fail()) {
          TEST_LOG_ERROR("Unable to Determine Status Code: " << current_line);
        }
      } else if (current_line.compare(0, 1, "\r") == 0) {
        while (std::getline(lines, current_line)) {
          request->response.message.append(test::Utils::trim(current_line));
        }
      } else if (!request->response.message.empty()) {
        request->response.message.append(test::Utils::trim(current_line));
      }
    }
  } else if (buffer_length < 0) {
    uv_close(reinterpret_cast<uv_handle_t*>(stream), NULL);
  }

  // Clean up the memory allocated
  delete[] buffer->base;
}

const std::string RestClient::generate_http_message(const Request& request) {
  // Determine the method of the the request
  std::stringstream message;
  if (request.method == Request::HTTP_METHOD_DELETE) {
    message << "DELETE";
  } else if (request.method == Request::HTTP_METHOD_GET) {
    message << "GET";
  } else if (request.method == Request::HTTP_METHOD_POST) {
    message << "POST";
  }
  message << " /" << request.endpoint << " HTTP/1.1" << HTTP_EOL;

  // Generate the headers
  bool is_post = request.method == Request::HTTP_METHOD_POST;
  message << "Host: " << request.address << ":" << request.port << HTTP_EOL
          << (is_post ? "Content-Type: application/json" HTTP_EOL : "")
          << "Content-Length: " << ((is_post) ? request.content.size() : 0) << HTTP_EOL
          << "Connection: close" << HTTP_EOL << HTTP_EOL << (is_post ? request.content : "");

  // Return the HTTP message
  TEST_LOG("[HTTP Message]: " << message.str());
  return message.str();
}
