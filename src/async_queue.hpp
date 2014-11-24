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

#include <boost/atomic.hpp>

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
      // There's something in uv_async_send() that can be reordered
      // before the item is written into the queue (the uv_async_t
      // "pending" flag?). This fence prevents this write from
      // being reordered before the  item is enqueued. Without this
      // the dequeuing thread sometimes does't see the item  in the
      // queue when responding to an async event (causing a request
      // to hang).
      //
      // On x86: "Loads may be reordered with older stores to different locations"
      //
      // This is mostly necessary for SPSCQueue<> as it doesn't emit a
      // fence (on x86). MPMCQueue<> uses a atomic compare exchange
      // which emits a full fence (via a "lock" instruction on x86)
      // making this not necessary on x86, but possible on other platforms.
      boost::atomic_thread_fence(boost::memory_order_seq_cst);
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
