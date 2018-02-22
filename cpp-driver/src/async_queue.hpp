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

#ifndef __CASS_ASYNC_QUEUE_HPP_INCLUDED__
#define __CASS_ASYNC_QUEUE_HPP_INCLUDED__

#include "async.hpp"

namespace cass {

template <typename Q>
class AsyncQueue {
public:
  AsyncQueue(size_t queue_size)
      : queue_(queue_size) {}

  int init(uv_loop_t* loop, void* data, Async::Callback cb) {
    return async_.start(loop, data, cb);
  }

  void close_handles() { async_.close_handle(); }

  void send() { async_.send(); }

  bool enqueue(const typename Q::EntryType& data) {
    if (queue_.enqueue(data)) {
      // uv_async_send() makes no guarantees about synchronization so it may
      // be necessary to use a memory fence to make sure stores happen before
      // the event loop wakes up and runs the async callback.
      Q::memory_fence();
      send();
      return true;
    }
    return false;
  }

  bool dequeue(typename Q::EntryType& data) { return queue_.dequeue(data); }

  // Testing only
  bool is_empty() const { return queue_.is_empty(); }

private:
  Async async_;
  Q queue_;
};

} // namespace cass

#endif
