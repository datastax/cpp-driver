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

#ifndef __CASS_EVENT_THREAD_HPP_INCLUDED__
#define __CASS_EVENT_THREAD_HPP_INCLUDED__

#include "loop_thread.hpp"
#include "async_queue.hpp"
#include "mpmc_queue.hpp"
#include "scoped_ptr.hpp"

#include <uv.h>

namespace cass {

template <class E>
class EventThread : public LoopThread {
public:
  int init(size_t queue_size) {
    event_queue_.reset(new AsyncQueue<MPMCQueue<E> >(queue_size));
    return event_queue_->init(loop(), this, on_event_internal);
  }

  void close_handles() { event_queue_->close_handles(); }

  bool send_event_async(const E& event) { return event_queue_->enqueue(event); }

  virtual void on_event(const E& event) = 0;

private:
  static void on_event_internal(uv_async_t* async, int status) {
    EventThread* thread = static_cast<EventThread*>(async->data);
    E event;
    while (thread->event_queue_->dequeue(event)) {
      thread->on_event(event);
    }
  }

  ScopedPtr<AsyncQueue<MPMCQueue<E> > > event_queue_;
};

} // namespace cass

#endif
