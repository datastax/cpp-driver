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

#include "request_queue.hpp"

#include "event_loop.hpp"
#include "pooled_connection.hpp"

namespace cass {

template <class C>
static void set_pointer_keys(C& container) {
  container.set_empty_key(reinterpret_cast<typename C::key_type>(0x0));
  container.set_deleted_key(reinterpret_cast<typename C::key_type>(0x1));
}

RequestQueue::RequestQueue()
 : is_flushing_(false)
 , is_closing_(false)
 , flushes_without_writes_(0) {
  set_pointer_keys(connections_);
}

int RequestQueue::init(EventLoop* event_loop, size_t queue_size) {
  queue_.reset(Memory::allocate<MPMCQueue<Item> >(queue_size));
  return async_.start(event_loop->loop(), this, on_flush);
}

void RequestQueue::close_handles() {
  is_closing_.store(true);
  async_.send();
}

bool RequestQueue::write(PooledConnection* connection, RequestCallback* callback) {
  Item item;
  item.callback = callback;
  item.connection = connection;

  connection->inc_ref();
  callback->inc_ref();
  if (!queue_->enqueue(item)) {
    connection->dec_ref();
    callback->dec_ref();
    return false;
  }

  // Only signal the request queue if it's not already processing requests.
  bool expected = false;
  if (!is_flushing_.load() && is_flushing_.compare_exchange_strong(expected, true)) {
    async_.send();
  }

  return true;
}

void RequestQueue::on_flush(Async* async) {
  RequestQueue * flusher = static_cast<RequestQueue*>(async->data());
  flusher->handle_flush();
}

void RequestQueue::on_flush_timer(Timer* timer) {
  RequestQueue * flusher = static_cast<RequestQueue*>(timer->data());
  flusher->handle_flush();
}

void RequestQueue::handle_flush() {
  const int new_request_ratio = 50; // TODO: Determine if this is useful

  bool writes_done = false;
  uint64_t start_time_ns = uv_hrtime();

  Item item;
  while (queue_->dequeue(item)) {
    if (!item.connection->is_closing(PooledConnection::Protected())) {
      int32_t result = item.connection->write(item.callback, PooledConnection::Protected());
      // TODO: Handle no streams available error
      if (result > 0) {
        writes_done = true;
        Set::iterator it = connections_.find(item.connection);
        if (it == connections_.end()) {
          item.connection->inc_ref();
          connections_.insert(item.connection);
        }
      }
    }

    item.callback->dec_ref();
    item.connection->dec_ref();
  }

  for (Set::iterator it = connections_.begin(),
       end = connections_.end(); it != end; ++it) {
    (*it)->flush(PooledConnection::Protected());
    (*it)->dec_ref();
  }
  connections_.clear();

  if (is_closing_.load()) {
    async_.close_handle();
    timer_.close_handle();
    return;
  }

  if (writes_done) {
    flushes_without_writes_ = 0;
  } else {
    // Determine if a another flush should be scheduled.
    if (++flushes_without_writes_ > 5) {
      is_flushing_.store(false);
      bool expected = false;
      if (queue_->is_empty() || !is_flushing_.compare_exchange_strong(expected, true)) {
        return;
      }
    }
  }

  uint64_t flush_time_ns = uv_hrtime() - start_time_ns;
  uint64_t processing_time_ns = flush_time_ns * (100 - new_request_ratio) / new_request_ratio;
  if (processing_time_ns >= 1000000) { // Schedule another flush to be run in the future.
    timer_.start(async_.loop(), (processing_time_ns + 500000) / 1000000, this, on_flush_timer);
  } else {
    async_.send(); // Schedule another flush to be run immediately.
  }
}

RequestQueueManager::RequestQueueManager(EventLoopGroup* event_loop_group)
  : storage_(event_loop_group->size())
  , event_loop_group_(event_loop_group) {
  set_pointer_keys(request_queues_);
}

int RequestQueueManager::init(size_t queue_size) {
  for (size_t i = 0; i < storage_.size(); ++i) {
    RequestQueue* request_queue = &storage_[i];
    EventLoop* event_loop = event_loop_group_->get(i);
    int rc = request_queue->init(event_loop, queue_size);
    if (rc != 0) return rc;
    request_queues_[event_loop] = request_queue;
  }
  return 0;
}

void RequestQueueManager::close_handles() {
  for (size_t i = 0; i < storage_.size(); ++i) {
    storage_[i].close_handles();
  }
}

RequestQueue* RequestQueueManager::get(EventLoop* event_loop) const {
  Map::const_iterator it = request_queues_.find(event_loop);
  assert(it != request_queues_.end() && "Request queue not found for event loop");
  return it->second;
}

} // namespace cass
