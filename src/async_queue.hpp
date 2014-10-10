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

#ifndef __CASS_ASYNC_QUEUE_HPP_INCLUDED__
#define __CASS_ASYNC_QUEUE_HPP_INCLUDED__

#include "common.hpp"

#include <uv.h>

namespace cass {

template <typename Q>
class AsyncQueue {
public:
  AsyncQueue(size_t queue_size)
      : queue_(queue_size) {
    async_.data = this;
  }

  int init(uv_loop_t* loop, void* data, uv_async_cb async_cb) {
    async_.data = data;
    return uv_async_init(loop, &async_, async_cb);
  }

  void close_handles() {
    uv_close(copy_cast<uv_async_t*, uv_handle_t*>(&async_), NULL);
  }

  void send() {
    uv_async_send(&async_);
  }

  bool enqueue(const typename Q::EntryType& data) {
    if (queue_.enqueue(data)) {
      uv_async_send(&async_);
      return true;
    }
    return false;
  }

  bool dequeue(typename Q::EntryType& data) { return queue_.dequeue(data); }

private:
  uv_async_t async_;
  Q queue_;
};

} // namespace cass

#endif
