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

#ifndef __CASS_REQUEST_CALLBACK_HPP_INCLUDED__
#define __CASS_REQUEST_CALLBACK_HPP_INCLUDED__

#include "buffer.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "utils.hpp"
#include "list.hpp"
#include "prepared.hpp"
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
class Pool;
class PreparedMetadata;
class ResponseMessage;
class ResultResponse;

typedef std::vector<uv_buf_t> UvBufVec;

/**
 * A wrapper class for keeping a request's state grouped together with the
 * request object. This is necessary because a request object is immutable
 * when it's being executed.
 */
class RequestWrapper {
public:
  RequestWrapper(const Request::ConstPtr &request)
    : request_(request)
    , consistency_(CASS_DEFAULT_CONSISTENCY)
    , serial_consistency_(CASS_DEFAULT_SERIAL_CONSISTENCY)
    , request_timeout_ms_(CASS_DEFAULT_REQUEST_TIMEOUT_MS)
    , timestamp_(CASS_INT64_MIN) { }

  void init(const Config& config,
            const PreparedMetadata& prepared_metadata);

  const Request::ConstPtr& request() const {
    return request_;
  }

  CassConsistency consistency() const {
    if (request()->consistency() != CASS_CONSISTENCY_UNKNOWN) {
      return request()->consistency();
    }
    return consistency_;
  }

  CassConsistency serial_consistency() const {
    if (request()->serial_consistency() != CASS_CONSISTENCY_UNKNOWN) {
      return request()->serial_consistency();
    }
    return serial_consistency_;
  }

  uint64_t request_timeout_ms() const {
    if (request()->request_timeout_ms() != CASS_UINT64_MAX) {
      return request()->request_timeout_ms();
    }
    return request_timeout_ms_;
  }

  int64_t timestamp() const {
    if (request()->timestamp() != CASS_INT64_MIN) {
      return request()->timestamp();
    }
    return timestamp_;
  }

  const RetryPolicy::Ptr& retry_policy() const {
    if (request()->retry_policy()) {
      return request()->retry_policy();
    }
    return retry_policy_;
  }

  const PreparedMetadata::Entry::Ptr& prepared_metadata_entry() const {
    return prepared_metadata_entry_;
  }

private:
  Request::ConstPtr request_;
  CassConsistency consistency_;
  CassConsistency serial_consistency_;
  uint64_t request_timeout_ms_;
  int64_t timestamp_;
  RetryPolicy::Ptr retry_policy_;
  PreparedMetadata::Entry::Ptr prepared_metadata_entry_;
};

class RequestCallback : public RefCounted<RequestCallback>, public List<RequestCallback>::Node {
public:
  typedef SharedRefPtr<RequestCallback> Ptr;
  typedef std::vector<Ptr> Vec;

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

  RequestCallback(const RequestWrapper& wrapper)
    : wrapper_(wrapper)
    , connection_(NULL)
    , stream_(-1)
    , state_(REQUEST_STATE_NEW)
    , retry_consistency_(CASS_CONSISTENCY_UNKNOWN) { }

  virtual ~RequestCallback() { }

  void start(Connection* connection, int stream);

  virtual void on_retry_current_host() = 0;
  virtual void on_retry_next_host() = 0;

  // One of these methods is called to finish a request
  virtual void on_set(ResponseMessage* response) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_cancel() = 0;

  int32_t encode(int version, int flags, BufferVec* bufs);

  const Request* request() const { return wrapper_.request().get(); }

  bool skip_metadata() const;

  CassConsistency consistency() {
    // The retry consistency takes the highest priority
    if (retry_consistency_ != CASS_CONSISTENCY_UNKNOWN) {
      return retry_consistency_;
    }
    return wrapper_.consistency();
  }

  CassConsistency serial_consistency() {
    return wrapper_.serial_consistency();
  }

  uint64_t request_timeout_ms() {
    return wrapper_.request_timeout_ms();
  }

 int64_t timestamp() {
   return wrapper_.timestamp();
 }

 const RetryPolicy::Ptr& retry_policy() {
   return wrapper_.retry_policy();
 }

  const PreparedMetadata::Entry::Ptr& prepared_metadata_entry() const {
    return wrapper_.prepared_metadata_entry();
  }

  void set_retry_consistency(CassConsistency cl) { retry_consistency_ = cl; }

  Connection* connection() const { return connection_; }

  int stream() const { return stream_; }

  State state() const { return state_; }
  void set_state(State next_state);

  ResponseMessage* read_before_write_response() const {
    return read_before_write_response_.get();
  }

  void set_read_before_write_response(ResponseMessage* response) {
    read_before_write_response_.reset(response);
  }

protected:
  // Called right before a request is written to a host.
  virtual void on_start() = 0;

private:
  const RequestWrapper wrapper_;
  Connection* connection_;
  int stream_;
  State state_;
  CassConsistency retry_consistency_;
  ScopedPtr<ResponseMessage> read_before_write_response_;

private:
  DISALLOW_COPY_AND_ASSIGN(RequestCallback);
};

class SimpleRequestCallback : public RequestCallback {
public:
  SimpleRequestCallback(const Request::ConstPtr& request)
    : RequestCallback(RequestWrapper(request)) { }

protected:
  virtual void on_internal_set(ResponseMessage* response) = 0;
  virtual void on_internal_error(CassError code, const std::string& message) = 0;
  virtual void on_internal_timeout() = 0;

private:
  virtual void on_start();

  virtual void on_retry_current_host();
  virtual void on_retry_next_host();

  virtual void on_set(ResponseMessage* response);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_cancel();

  static void on_timeout(Timer* timer);

private:
  Timer timer_;
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
