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

#ifndef __CASS_HANDLER_HPP_INCLUDED__
#define __CASS_HANDLER_HPP_INCLUDED__

#include "buffer.hpp"
#include "constants.hpp"
#include "cassandra.h"
#include "utils.hpp"
#include "list.hpp"
#include "request.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"

#include <string>
#include <uv.h>

namespace cass {

class Config;
class Connection;
class ResponseMessage;

typedef std::vector<uv_buf_t> UvBufVec;

class Handler : public RefCounted<Handler>, public List<Handler>::Node {
public:
  enum State {
    REQUEST_STATE_NEW,
    REQUEST_STATE_WRITING,
    REQUEST_STATE_READING,
    REQUEST_STATE_TIMEOUT,
    REQUEST_STATE_TIMEOUT_WRITE_OUTSTANDING,
    REQUEST_STATE_READ_BEFORE_WRITE,
    REQUEST_STATE_RETRY_WRITE_OUTSTANDING,
    REQUEST_STATE_DONE
  };

  Handler(const Request* request)
    : request_(request)
    , connection_(NULL)
    , stream_(-1)
    , state_(REQUEST_STATE_NEW)
    , cl_(CASS_CONSISTENCY_UNKNOWN)
    , timestamp_(CASS_INT64_MIN)
    , start_time_ns_(0) { }

  virtual ~Handler() {}

  int32_t encode(int version, int flags, BufferVec* bufs);

  virtual void on_set(ResponseMessage* response) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_timeout() = 0;

  virtual void retry() { }

  const Request* request() const { return request_.get(); }

  Connection* connection() const { return connection_; }

  void set_connection(Connection* connection) {
    connection_ = connection;
  }

  int stream() const { return stream_; }

  void set_stream(int stream) {
    stream_ = stream;
  }

  State state() const { return state_; }

  void set_state(State next_state);

  void start_timer(uv_loop_t* loop, uint64_t timeout, void* data,
                   Timer::Callback cb) {
    timer_.start(loop, timeout, data, cb);
  }

  void stop_timer() {
    timer_.stop();
  }

  CassConsistency consistency() const {
    return cl_ != CASS_CONSISTENCY_UNKNOWN ? cl_ : request()->consistency();
  }

  void set_consistency(CassConsistency cl) { cl_ = cl; }

  int64_t timestamp() const {
    return timestamp_;
  }

  void set_timestamp(int64_t timestamp) {
    timestamp_ = timestamp;
  }

  uint64_t request_timeout_ms(const Config& config) const;

  uint64_t start_time_ns() const { return start_time_ns_; }

  Request::EncodingCache* encoding_cache() { return &encoding_cache_; }

protected:
  ScopedRefPtr<const Request> request_;
  Connection* connection_;

private:
  Timer timer_;
  int stream_;
  State state_;
  CassConsistency cl_;
  int64_t timestamp_;
  uint64_t start_time_ns_;
  Request::EncodingCache encoding_cache_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handler);
};

} // namespace cass

#endif
