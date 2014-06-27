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

#ifndef __CASS_WRITER_HPP_INCLUDED__
#define __CASS_WRITER_HPP_INCLUDED__

#include "macros.hpp"
#include "buffer.hpp"

#include "third_party/boost/boost/function.hpp"

#include <uv.h>
#include <vector>
#include <functional>

namespace cass {

class Writer {
public:
  typedef boost::function1<void, Writer*> Callback;

  enum Status { WRITING, FAILED, SUCCESS };

  Status status() { return status_; }
  void* data() { return data_; }

  static void write(uv_stream_t* handle, const BufferVec* bufs, void* data, Callback cb) {
    Writer* writer = new Writer(bufs, data, cb);
    int rc =
        uv_write(&writer->req_, handle, writer->uv_bufs_.data(), writer->uv_bufs_.size(), on_write);
    if (rc != 0) {
      writer->status_ = FAILED;
      writer->cb_(writer);
      delete writer;
    }
  }

private:
  static void on_write(uv_write_t* req, int status) {
    Writer* writer = static_cast<Writer*>(req->data);
    if (status != 0) {
      writer->status_ = FAILED;
    } else {
      writer->status_ = SUCCESS;
    }
    writer->cb_(writer);
    delete writer;
  }

private:
  Writer(const BufferVec* bufs, void* data, Callback cb)
      : bufs_(bufs)
      , data_(data)
      , cb_(cb)
      , status_(WRITING) {
    uv_bufs_.reserve(bufs->size());
    for(BufferVec::const_iterator it = bufs->begin(), end = bufs->end();
        it != end; ++it) {
      uv_bufs_.push_back(uv_buf_init(const_cast<char*>(it->data()), it->size()));
    }
    req_.data = this;
  }

  uv_write_t req_;
  ScopedPtr<const BufferVec> bufs_;
  std::vector<uv_buf_t> uv_bufs_;
  void* data_;
  Callback cb_;
  Status status_;
};
}

#endif
