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

#include "constants.hpp"
#include "future.hpp"
#include "host.hpp"
#include "request.hpp"
#include "response.hpp"
#include "response_callback.hpp"
#include "scoped_ptr.hpp"

#include "third_party/boost/boost/function.hpp"

#include <list>
#include <string>

namespace cass {

class Timer;

class ResponseFuture : public ResultFuture<Response> {
public:
  ResponseFuture()
      : ResultFuture<Response>(CASS_FUTURE_TYPE_RESPONSE) {}

  std::string statement;
};

class RequestHandler : public ResponseCallback {
public:
  typedef boost::function1<void, RequestHandler*> Callback;
  typedef boost::function2<void, RequestHandler*, RetryType> RetryCallback;

  RequestHandler(const Request* request)
      : timer(NULL)
      , request_(request)
      , future_(new ResponseFuture()) {
    future_->retain();
  }

  virtual const Request* request() const { return request_.get(); }

  virtual void on_set(ResponseMessage* response);

  virtual void on_error(CassError code, const std::string& message);

  virtual void on_timeout();

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
  ScopedRefPtr<const Request> request_;
  ResponseFuture* future_;
  RetryCallback retry_callback_;
  Callback finished_callback_;
};
}

#endif
