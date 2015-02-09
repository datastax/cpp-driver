/*
  Copyright (c) 2014-2015 DataStax

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

#ifndef __CASS_SPSC_QUEUE_HPP_INCLUDED__
#define __CASS_SPSC_QUEUE_HPP_INCLUDED__

#include <assert.h>

#include "common.hpp"
#include "macros.hpp"

#include <boost/atomic.hpp>
#include <boost/type_traits/alignment_of.hpp>
#include <boost/aligned_storage.hpp>

namespace cass {

template <typename T>
class SPSCQueue {
public:
  typedef T EntryType;

  SPSCQueue(size_t size)
      : size_(next_pow_2(size))
      , mask_(size_ - 1)
      , buffer_(reinterpret_cast<T*>(
            new SPSCQueueAlignedEntry[size_]))
      , tail_(0)
      , head_(0) {}

  ~SPSCQueue() { delete[] buffer_; }

  bool enqueue(const T& input) {
    const size_t pos = tail_.load(boost::memory_order_relaxed);
    const size_t next_pos = (pos + 1) & mask_;
    if (next_pos == head_.load(boost::memory_order_acquire)) {
      return false;
    }
    buffer_[pos] = input;
    tail_.store(next_pos, boost::memory_order_release);
    return true;
  }

  bool dequeue(T& output) {
    const size_t pos = head_.load(boost::memory_order_relaxed);
    if (pos == tail_.load(boost::memory_order_acquire)) {
      return false;
    }
    output = buffer_[pos];
    head_.store((pos + 1) & mask_, boost::memory_order_release);
    return true;
  }

  bool is_empty() {
    return head_.load(boost::memory_order_acquire) ==
        tail_.load(boost::memory_order_acquire);
  }

private:
  typedef typename boost::aligned_storage<
      sizeof(T), boost::alignment_of<T>::value>::type SPSCQueueAlignedEntry;

  typedef char cache_line_pad_t[64];

  cache_line_pad_t pad0_;
  const size_t size_;
  const size_t mask_;
  T* const buffer_;

  cache_line_pad_t pad1_;
  boost::atomic<size_t> tail_;

  cache_line_pad_t pad2_;
  boost::atomic<size_t> head_;

  DISALLOW_COPY_AND_ASSIGN(SPSCQueue);
};

} // namespace cass

#endif
