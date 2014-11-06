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

class RequestWriter {
public:
  typedef boost::function1<void, RequestWriter*> Callback;

  RequestWriter()
      : data_(NULL)
      , cb_(NULL)
      , status_(WRITING) {
    req_.data = this;
  }

  enum Status { WRITING, FAILED, SUCCESS };

  Status status() { return status_; }
  void* data() { return data_; }
  BufferVec& bufs() { return bufs_; }

  void write(uv_stream_t* stream, void* data, Callback cb) {
    size_t bufs_size = bufs_.size();
    ScopedPtr<uv_buf_t[]> uv_bufs(new uv_buf_t[bufs_size]);

    for (size_t i = 0; i < bufs_size; ++i) {
      Buffer& buf = bufs_[i];
      uv_bufs[i] = uv_buf_init(const_cast<char*>(buf.data()), buf.size());
    }

    data_ = data;
    cb_ = cb;

    int rc = uv_write(&req_, stream, uv_bufs.get(), bufs_size, on_write);
    if (rc != 0) {
      status_ = FAILED;
      cb_(this);
    }
  }

private:
  static void on_write(uv_write_t* req, int status) {
    RequestWriter* writer = static_cast<RequestWriter*>(req->data);
    if (status != 0) {
      writer->status_ = FAILED;
    } else {
      writer->status_ = SUCCESS;
    }
    writer->cb_(writer);
  }

private:
  uv_write_t req_;
  BufferVec bufs_;
  void* data_;
  Callback cb_;
  Status status_;
};

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

  Handler()
    : stream_(-1)
    , state_(REQUEST_STATE_NEW) {}

  virtual ~Handler() {}

  virtual const Request* request() const = 0;

  bool encode(int version, int flags);
  void write(uv_stream_t* stream, void* data, RequestWriter::Callback cb);

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
  RequestWriter writer_;
  int8_t stream_;
  State state_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handler);
};

} // namespace cass

#endif
