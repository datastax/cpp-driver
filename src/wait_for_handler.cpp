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

#include "wait_for_handler.hpp"

#include "cassandra.h"
#include "query_request.hpp"

#include <iomanip>

using namespace datastax;
using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

/**
 * A callback that handles a chain of requests on behalf of the wait handler.
 */
class WaitForCallback : public ChainedRequestCallback {
public:
  WaitForCallback(const String& key, const Request::ConstPtr& request,
                  const WaitForHandler::Ptr& handler)
      : ChainedRequestCallback(key, request)
      , handler_(handler) {}

private:
  virtual void on_chain_write(Connection* connection);
  virtual void on_chain_set();
  virtual void on_chain_error(CassError code, const String& message);
  virtual void on_chain_timeout();

private:
  WaitForHandler::Ptr handler_;
};

void WaitForCallback::on_chain_write(Connection* connection) { handler_->start(connection); }

}}} // namespace datastax::internal::core

void WaitForCallback::on_chain_set() {
  if (handler_->is_finished_) return;

  if (handler_->on_set(Ptr(this))) {
    handler_->finish();
  } else {
    handler_->schedule();
  }
}

void WaitForCallback::on_chain_error(CassError code, const String& message) {
  if (handler_->is_finished_) return;

  OStringStream ss;
  ss << message << " (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code
     << ")";
  handler_->on_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_ERROR, ss.str());
  handler_->finish();
}

void WaitForCallback::on_chain_timeout() {
  if (handler_->is_finished_) return;

  handler_->on_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_TIMEOUT, "Request timed out");
  handler_->schedule();
}

ChainedRequestCallback::Ptr WaitForHandler::callback(const WaitforRequestVec& requests) {
  requests_ = requests;
  ChainedRequestCallback::Ptr chain;
  for (WaitforRequestVec::const_iterator it = requests.begin(), end = requests.end(); it != end;
       ++it) {
    if (!chain) {
      chain.reset(new WaitForCallback(it->first, it->second, Ptr(this)));
    } else {
      chain = chain->chain(it->first, it->second);
    }
  }
  return chain;
}

WaitForHandler::WaitForRequest WaitForHandler::make_request(const String& key,
                                                            const String& query) {
  QueryRequest::Ptr request(new QueryRequest(query));
  request->set_request_timeout_ms(request_timeout_ms());
  return WaitForRequest(key, request);
}

void WaitForHandler::start(Connection* connection) {
  if (!connection_ && !is_finished_) { // Start only once
    inc_ref();                         // Reference for the event loop
    connection_.reset(connection);
    timer_.start(connection_->loop(), max_wait_time_ms_,
                 bind_callback(&WaitForHandler::on_timeout, this));
  }
}

void WaitForHandler::schedule() {
  assert(!is_finished_ && "This shouldn't be called if the handler is finished");
  retry_timer_.start(connection_->loop(), retry_wait_time_ms_,
                     bind_callback(&WaitForHandler::on_retry_timeout, this));
}

void WaitForHandler::finish() {
  assert(!is_finished_ && "This shouldn't be called more than once");
  is_finished_ = true;
  request_handler_->set_response(current_host_, response_);
  if (connection_) {
    connection_.reset();
    retry_timer_.stop();
    timer_.stop();
    dec_ref();
  }
}

void WaitForHandler::on_retry_timeout(Timer* timer) {
  if (is_finished_) return;

  if (connection_->is_closing()) {
    on_error(WaitForHandler::WAIT_FOR_ERROR_CONNECTION_CLOSED, "Connection closed");
    finish();
  } else if (connection_->write_and_flush(callback(requests_)) ==
             core::Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS) {
    on_error(WaitForHandler::WAIT_FOR_ERROR_NO_STREAMS, "No streams available");
    finish();
  }
}

void WaitForHandler::on_timeout(Timer* timer) {
  if (is_finished_) return;

  on_error(WaitForHandler::WAIT_FOR_ERROR_TIMEOUT, "Timed out");
  finish();
}

WaitForHandler::WaitForHandler(const RequestHandler::Ptr& request_handler,
                               const Host::Ptr& current_host, const Response::Ptr& response,
                               uint64_t max_wait_time_ms, uint64_t retry_wait_time_ms)
    : is_finished_(false)
    , start_time_ms_(get_time_since_epoch_ms())
    , max_wait_time_ms_(max_wait_time_ms)
    , retry_wait_time_ms_(retry_wait_time_ms)
    , request_handler_(request_handler)
    , current_host_(current_host)
    , response_(response) {}
