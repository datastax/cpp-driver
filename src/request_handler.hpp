/*
  Copyright 2014 DataStax

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

#ifndef __CASS_REQUEST_HANDLER_HPP_INCLUDED__
#define __CASS_REQUEST_HANDLER_HPP_INCLUDED__

#include "response_callback.hpp"
#include "request_future.hpp"


namespace cass {

enum RetryType {
  RETRY_WITH_CURRENT_HOST,
  RETRY_WITH_NEXT_HOST
};

class RequestHandler;
typedef std::function<void(RequestHandler* request_handler,
                           RetryType type)> RetryCallback;

class RequestHandler : public ResponseCallback {
  public:
    RequestHandler(Message* request)
      : timer(nullptr)
      , request_(request)
      , future_(new RequestFuture()) { }

    ~RequestHandler() {
      // We don't own this memory (external statments)
      request_->body.release();
    }

    virtual Message* request() const {
      return request_.get();
    }

    virtual void on_set(Message* response) {
      switch(response->opcode) {
        case CQL_OPCODE_RESULT:
          future_->set_result(response->body.release());
          break;
        case CQL_OPCODE_ERROR: {
          ErrorResponse* error = static_cast<ErrorResponse*>(response->body.get());
          future_->set_error(CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error->code),
                             error->message);
        }
          break;
        default:
          // TODO(mpenick): Get the host for errors
          future_->set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Unexpected response");
          break;
      }
    }

    virtual void on_error(CassError code, const std::string& message) {
      future_->set_error(code, message);
    }

    virtual void on_timeout() {
      // TODO(mpenick): Get the host for errors
      future_->set_error(CASS_ERROR_LIB_REQUEST_TIMEOUT, "Request timed out");
    }

    RequestFuture* future() { return future_; }

    bool get_current_host(Host* host) {
      if(hosts.empty()) {
        return false;
      }
      *host = hosts.front();
      return true;
    }

    void next_host() {
      if(hosts.empty()) {
        return;
      }
      hosts_attempted_.push_back(hosts.front());
      hosts.pop_front();
    }

  public:
    Timer* timer;
    std::list<Host> hosts;

  private:
    std::list<Host> hosts_attempted_;
    std::unique_ptr<Message> request_;
    RequestFuture* future_;

};

}

#endif
