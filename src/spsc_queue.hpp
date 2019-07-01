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

  Note:
  A combination of the algorithms described by the circular buffers
  documentation found in the Linux kernel, and the bounded MPMC queue
  by Dmitry Vyukov[1]. Implemented in pure C++11. Should work across
  most CPU architectures.

  [1]
  http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
*/

#ifndef DATASTAX_INTERNAL_SPSC_QUEUE_HPP
#define DATASTAX_INTERNAL_SPSC_QUEUE_HPP

#include <assert.h>

#include "atomic.hpp"
#include "driver_config.hpp"
#include "macros.hpp"
#include "scoped_ptr.hpp"
#include "utils.hpp"

namespace datastax { namespace internal { namespace core {

template <typename T>
class SPSCQueue {
public:
  typedef T EntryType;

  SPSCQueue(size_t size)
      : size_(next_pow_2(size))
      , mask_(size_ - 1)
      , buffer_(new T[size])
      , tail_(0)
      , head_(0) {}

  bool enqueue(const T& input) {
    const size_t pos = tail_.load(MEMORY_ORDER_RELAXED);
    const size_t next_pos = (pos + 1) & mask_;
    if (next_pos == head_.load(MEMORY_ORDER_ACQUIRE)) {
      return false;
    }
    buffer_[pos] = input;
    tail_.store(next_pos, MEMORY_ORDER_RELEASE);
    return true;
  }

  bool dequeue(T& output) {
    const size_t pos = head_.load(MEMORY_ORDER_RELAXED);
    if (pos == tail_.load(MEMORY_ORDER_ACQUIRE)) {
      return false;
    }
    output = buffer_[pos];
    head_.store((pos + 1) & mask_, MEMORY_ORDER_RELEASE);
    return true;
  }

  bool is_empty() { return head_.load(MEMORY_ORDER_ACQUIRE) == tail_.load(MEMORY_ORDER_ACQUIRE); }

  static void memory_fence() {
    // Internally, libuv has a "pending" flag check whose load can be reordered
    // before storing the data into the queue causing the data in the queue
    // not to be consumed. This fence ensures that the load happens after the
    // data has been store in the queue.
#if defined(HAVE_BOOST_ATOMIC) || defined(HAVE_STD_ATOMIC)
    atomic_thread_fence(MEMORY_ORDER_SEQ_CST);
#endif
  }

private:
  typedef char cache_line_pad_t[64];

  cache_line_pad_t pad0_;
  const size_t size_;
  const size_t mask_;
  ScopedArray<T> buffer_;

  cache_line_pad_t pad1_;
  Atomic<size_t> tail_;

  cache_line_pad_t pad2_;
  Atomic<size_t> head_;

  DISALLOW_COPY_AND_ASSIGN(SPSCQueue);
};

}}} // namespace datastax::internal::core

#endif
