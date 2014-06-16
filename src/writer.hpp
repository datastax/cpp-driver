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

#include <uv.h>

#include <vector>

namespace cass {

class Writer {
public:
  typedef std::function<void(Writer*)> Callback;
  typedef std::vector<uv_buf_t> Bufs;

  enum Status { WRITING, FAILED, SUCCESS };

  Status status() { return status_; }
  void* data() { return data_; }

  static void write(uv_stream_t* handle, Bufs* bufs, void* data, Callback cb) {
    Writer* writer = new Writer(bufs, data, cb);
    int rc =
        uv_write(&writer->req_, handle, bufs->data(), bufs->size(), on_write);
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
  Writer(Bufs* bufs, void* data, Callback cb)
      : bufs_(bufs)
      , data_(data)
      , cb_(cb)
      , status_(WRITING) {
    req_.data = this;
  }

  ~Writer() {
    for (uv_buf_t buf : *bufs_) {
      delete[] buf.base;
    }
    delete bufs_;
  }

  uv_write_t req_;
  Bufs* bufs_;
  void* data_;
  Callback cb_;
  Status status_;
};
}

#endif
