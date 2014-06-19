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
#include "future.hpp"
#include "message.hpp"
#include "timer.hpp"
#include "scoped_ptr.hpp"

#include "third_party/boost/boost/function.hpp"

namespace cass {

class ResponseFuture : public ResultFuture<Response> {
public:
  ResponseFuture()
      : ResultFuture(CASS_FUTURE_TYPE_RESPONSE) {}

  std::string statement;
};

enum RetryType { RETRY_WITH_CURRENT_HOST, RETRY_WITH_NEXT_HOST };

class RequestHandler : public ResponseCallback {
public:
  typedef boost::function1<void, RequestHandler*> Callback;
  typedef boost::function2<void, RequestHandler*, RetryType> RetryCallback;

  RequestHandler(Message* request)
      : timer(NULL)
      , request_(request)
      , future_(new ResponseFuture()) {
    future_->retain();
    request_->request_body->retain();
  }

  ~RequestHandler() {
    request_->request_body->release();
  }

  virtual Message* request() const { return request_.get(); }

  virtual void on_set(Message* response) {
    switch (response->opcode) {
      case CQL_OPCODE_RESULT:
        future_->set_result(response->response_body.release());
        break;
      case CQL_OPCODE_ERROR: {
        ErrorResponse* error =
            static_cast<ErrorResponse*>(response->response_body.get());
        future_->set_error(static_cast<CassError>(CASS_ERROR(
                               CASS_ERROR_SOURCE_SERVER, error->code)),
                           std::string(error->message, error->message_size));
      } break;
      default:
        // TODO(mpenick): Get the host for errors
        future_->set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                           "Unexpected response");
        break;
    }
    notify_finished();
    ;
  }

  virtual void on_error(CassError code, const std::string& message) {
    future_->set_error(code, message);
    notify_finished();
  }

  virtual void on_timeout() {
    // TODO(mpenick): Get the host for errors
    future_->set_error(CASS_ERROR_LIB_REQUEST_TIMED_OUT, "Request timed out");
    notify_finished();
  }

  void set_retry_callback(RetryCallback callback) {
    retry_callback_ = callback;
  }

  void retry(RetryType type) {
    if (retry_callback_) {
      retry_callback_(this, type);
    }
  }

  void set_finished_callback(Callback callback) {
    finished_callback_ = callback;
  }

  ResponseFuture* future() { return future_; }

  bool get_current_host(Host* host) {
    if (hosts.empty()) {
      return false;
    }
    *host = hosts.front();
    return true;
  }

  void next_host() {
    if (hosts.empty()) {
      return;
    }
    hosts_attempted_.push_back(hosts.front());
    hosts.pop_front();
  }

public:
  Timer* timer;
  std::list<Host> hosts;
  std::string keyspace;

private:
  void notify_finished() {
    if (finished_callback_) {
      finished_callback_(this);
    }
  }

  std::list<Host> hosts_attempted_;
  ScopedPtr<Message> request_;
  ResponseFuture* future_;
  RetryCallback retry_callback_;
  Callback finished_callback_;
};
}

#endif
