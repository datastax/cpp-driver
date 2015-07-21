/*
  Copyright (c) 2014-2015 DataStax

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
    REQUEST_STATE_DONE
  };

  Handler(const Request* request)
    : request_(request)
    , connection_(NULL)
    , stream_(-1)
    , state_(REQUEST_STATE_NEW) {}

  virtual ~Handler() {}

  int32_t encode(int version, int flags, BufferVec* bufs);

  virtual void on_set(ResponseMessage* response) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_timeout() = 0;

  const Request* request() const { return request_.get(); }

  Connection* connection() const { return connection_; }

  void set_connection(Connection* connection) {
    connection_ = connection;
  }

  int8_t stream() const { return stream_; }

  void set_stream(int8_t stream) {
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

  uint64_t start_time_ns() const { return start_time_ns_; }

  Request::EncodingCache* encoding_cache() { return &encoding_cache_; }

protected:
  ScopedRefPtr<const Request> request_;
  Connection* connection_;

private:
  Timer timer_;
  int16_t stream_;
  State state_;
  uint64_t start_time_ns_;
  Request::EncodingCache encoding_cache_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handler);
};

} // namespace cass

#endif
