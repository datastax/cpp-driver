/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_REQUEST_CALLBACK_HPP_INCLUDED__
#define __CASS_REQUEST_CALLBACK_HPP_INCLUDED__

#include "buffer.hpp"
#include "constants.hpp"
#include "cassandra.h"
#include "utils.hpp"
#include "list.hpp"
#include "request.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"

#include <string>
#include <uv.h>

namespace cass {

class Config;
class Connection;
class Metrics;
class ResponseMessage;
class ResultResponse;

typedef std::vector<uv_buf_t> UvBufVec;

class RequestCallback : public RefCounted<RequestCallback>, public List<RequestCallback>::Node {
public:
  typedef SharedRefPtr<RequestCallback> Ptr;

  enum State {
    REQUEST_STATE_NEW,
    REQUEST_STATE_WRITING,
    REQUEST_STATE_READING,
    REQUEST_STATE_READ_BEFORE_WRITE,
    REQUEST_STATE_FINISHED,
    REQUEST_STATE_CANCELLED,
    REQUEST_STATE_CANCELLED_WRITING,
    REQUEST_STATE_CANCELLED_READING,
    REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE
  };

  RequestCallback()
    : connection_(NULL)
    , stream_(-1)
    , state_(REQUEST_STATE_NEW)
    , cl_(CASS_CONSISTENCY_UNKNOWN) { }

  virtual ~RequestCallback() { }

  void start(Connection* connection, int stream);

  virtual void on_retry(bool use_next_host) = 0;

  // One of these methods is called to finish a request
  virtual void on_set(ResponseMessage* response) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_cancel() = 0;

  int32_t encode(int version, int flags, BufferVec* bufs);

  virtual int64_t timestamp() const { return request()->timestamp(); }

  Connection* connection() const { return connection_; }

  int stream() const { return stream_; }

  State state() const { return state_; }
  void set_state(State next_state);

  CassConsistency consistency() const {
    return cl_ != CASS_CONSISTENCY_UNKNOWN ? cl_ : request()->consistency();
  }

  void set_consistency(CassConsistency cl) { cl_ = cl; }

  ResponseMessage* read_before_write_response() const {
    return read_before_write_response_.get();
  }

  void set_read_before_write_response(ResponseMessage* response) {
    read_before_write_response_.reset(response);
  }

public:
  virtual const Request* request() const = 0;
  virtual Request::EncodingCache* encoding_cache() = 0;

protected:
  // Called right before a request is written to a host.
  virtual void on_start() = 0;

private:
  Connection* connection_;
  int stream_;
  State state_;
  CassConsistency cl_;
  ScopedPtr<ResponseMessage> read_before_write_response_;

private:
  DISALLOW_COPY_AND_ASSIGN(RequestCallback);
};

class SimpleRequestCallback : public RequestCallback {
public:
  SimpleRequestCallback(const Request::ConstPtr& request)
    : RequestCallback()
    , request_(request) { }

  virtual const Request* request() const { return request_.get(); }
  virtual Request::EncodingCache* encoding_cache() { return &encoding_cache_; }

protected:
  virtual void on_internal_set(ResponseMessage* response) = 0;
  virtual void on_internal_error(CassError code, const std::string& message) = 0;
  virtual void on_internal_timeout() = 0;

private:
  virtual void on_start();

  virtual void on_retry(bool use_next_host);

  virtual void on_set(ResponseMessage* response);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_cancel();

  static void on_timeout(Timer* timer);

private:
  Timer timer_;
  const Request::ConstPtr request_;
  Request::EncodingCache encoding_cache_;
};

class MultipleRequestCallback : public RefCounted<MultipleRequestCallback> {
public:
  typedef SharedRefPtr<MultipleRequestCallback> Ptr;
  typedef std::map<std::string, Response::Ptr> ResponseMap;

  MultipleRequestCallback(Connection* connection)
    : connection_(connection)
    , has_errors_or_timeouts_(false)
    , remaining_(0) { }

  virtual ~MultipleRequestCallback() { }

  static bool get_result_response(const ResponseMap& responses,
                                  const std::string& index,
                                  ResultResponse** response);

  void execute_query(const std::string& index, const std::string& query);

  virtual void on_set(const ResponseMap& responses) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_timeout() = 0;

  Connection* connection() { return connection_; }

private:
  class InternalCallback : public SimpleRequestCallback {
  public:
    InternalCallback(const MultipleRequestCallback::Ptr& parent,
                     const Request::ConstPtr& request,
                     const std::string& index);

  private:
    virtual void on_internal_set(ResponseMessage* response);
    virtual void on_internal_error(CassError code, const std::string& message);
    virtual void on_internal_timeout();

  private:
    MultipleRequestCallback::Ptr parent_;
    std::string index_;
  };

  Connection* connection_;
  bool has_errors_or_timeouts_;
  int remaining_;
  ResponseMap responses_;
};

} // namespace cass

#endif
