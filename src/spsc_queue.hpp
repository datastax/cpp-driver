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

#include <boost/atomic.hpp>
#include <boost/type_traits/alignment_of.hpp>
#include <boost/aligned_storage.hpp>

namespace cass {

template <typename T>
class SPSCQueue {
public:
  typedef T EntryType;

  SPSCQueue(size_t size)
      : _size(next_pow_2(size))
      , _mask(_size - 1)
      , _buffer(reinterpret_cast<T*>(
            // need one extra element for a guard
            new SPSCQueueAlignedEntry[_size + 1]))
      , _head(0)
      , _tail(0) {}

  ~SPSCQueue() { delete[] _buffer; }

  bool enqueue(const T& input) {
    const size_t head = _head.load(boost::memory_order_relaxed);

    if (((_tail.load(boost::memory_order_acquire) - (head + 1)) & _mask) >= 1) {
      _buffer[head & _mask] = input;
      _head.store(head + 1, boost::memory_order_release);
      return true;
    }
    return false;
  }

  bool dequeue(T& output) {
    const size_t tail = _tail.load(boost::memory_order_relaxed);

    if (((_head.load(boost::memory_order_acquire) - tail) & _mask) >= 1) {
      output = _buffer[_tail & _mask];
      _tail.store(tail + 1, boost::memory_order_release);
      return true;
    }
    return false;
  }

  bool is_empty() {
    return _tail.load(boost::memory_order_acquire) == _head.load(boost::memory_order_acquire);
  }

private:
  typedef typename boost::aligned_storage<
      sizeof(T), boost::alignment_of<T>::value>::type SPSCQueueAlignedEntry;

  typedef char cache_line_pad_t[64];

  cache_line_pad_t _pad0;
  const size_t _size;
  const size_t _mask;
  T* const _buffer;

  cache_line_pad_t _pad1;
  boost::atomic<size_t> _head;

  cache_line_pad_t _pad2;
  boost::atomic<size_t> _tail;

  SPSCQueue(const SPSCQueue&) {}
  void operator=(const SPSCQueue&) {}
};

} // namespace cass

#endif
