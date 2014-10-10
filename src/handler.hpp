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

#ifndef __CASS_HANDLER_HPP_INCLUDED__
#define __CASS_HANDLER_HPP_INCLUDED__

#include "buffer.hpp"
#include "cassandra.h"
#include "common.hpp"
#include "list.hpp"
#include "scoped_ptr.hpp"

#include <string>
#include <uv.h>

#include "third_party/boost/boost/function.hpp"

namespace cass {

class Config;
class Connection;
class Request;
class ResponseMessage;

typedef std::vector<uv_buf_t> UvBufVec;

struct RequestTimer {
  typedef boost::function1<void, RequestTimer*> Callback;

  RequestTimer()
    : handle_(NULL)
    , data_(NULL) { }

  ~RequestTimer() {
    if (handle_ != NULL) {
      uv_close(copy_cast<uv_timer_t*, uv_handle_t*>(handle_), on_close);
    }
  }

  void* data() const { return data_; }

  void start(uv_loop_t* loop, uint64_t timeout, void* data,
                   Callback cb) {
    if (handle_ == NULL) {
      handle_ = new  uv_timer_t;
      handle_->data = this;
      uv_timer_init(loop, handle_);
    }
    data_ = data;
    cb_ = cb;
    uv_timer_start(handle_, on_timeout, timeout, 0);
  }

  void stop() {
    if (handle_ != NULL) {
      uv_timer_stop(handle_);
    }
  }

  static void on_timeout(uv_timer_t* handle, int status) {
    RequestTimer* timer = static_cast<RequestTimer*>(handle->data);
    timer->cb_(timer);
  }

  static void on_close(uv_handle_t* handle) {
    delete copy_cast<uv_handle_t*, uv_timer_t*>(handle);
  }

private:
  uv_timer_t* handle_;
  void* data_;
  Callback cb_;
};

class Handler : public RefCounted<Handler>, public List<Handler>::Node {
public:
  enum State {
    REQUEST_STATE_NEW,
    REQUEST_STATE_WRITING,
    REQUEST_STATE_READING,
    REQUEST_STATE_WRITE_TIMEOUT,
    REQUEST_STATE_READ_TIMEOUT,
    REQUEST_STATE_READ_BEFORE_WRITE,
    REQUEST_STATE_DONE
  };

  Handler()
    : stream_(-1)
    , state_(REQUEST_STATE_NEW) {}

  virtual ~Handler() {}

  virtual const Request* request() const = 0;

  int32_t encode(int version, int flags, BufferVec* bufs) const;

  virtual void on_set(ResponseMessage* response) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_timeout() = 0;

  int8_t stream() const { return stream_; }

  void set_stream(int8_t stream) {
    stream_ = stream;
  }

  State state() const { return state_; }

  void set_state(State next_state);

  void start_timer(uv_loop_t* loop, uint64_t timeout, void* data,
                   RequestTimer::Callback cb) {
    timer_.start(loop, timeout, data, cb);
  }

  void stop_timer() {
    timer_.stop();
  }

private:
  RequestTimer timer_;
  int8_t stream_;
  State state_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handler);
};

} // namespace cass

#endif
