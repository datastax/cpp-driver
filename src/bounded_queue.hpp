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
  Algorithm described by the circular buffers documentation found in
  the Linux kernel
*/

#ifndef __CASS_BOUND_QUEUE_HPP_INCLUDED__
#define __CASS_BOUND_QUEUE_HPP_INCLUDED__

#include <assert.h>

#include "common.hpp"

namespace cass {

template <typename T>
class BoundedQueue {
public:
  BoundedQueue(size_t size)
      : _size(size)
      , _mask(_size - 1)
      , _buffer(reinterpret_cast<T*>(
            // need one extra element for a guard
            new BoundedQueueAlignedEntry[_size + 1]))
      , _head(0)
      , _tail(0) {
    // make sure it's a power of 2
    assert((_size != 0) && ((_size & (~_size + 1)) == _size));
  }

  ~BoundedQueue() { delete[] _buffer; }

  bool enqueue(T& input) {
    if (((_tail - (_head + 1)) & _mask) >= 1) {
      _buffer[_head & _mask] = input;
      _head++;
      return true;
    }
    return false;
  }

  bool dequeue(T& output) {
    if (((_head - _tail) & _mask) >= 1) {
      output = _buffer[_tail & _mask];
      _tail++;
      return true;
    }
    return false;
  }

private:
  typedef typename std::aligned_storage<
      sizeof(T), std::alignment_of<T>::value>::type BoundedQueueAlignedEntry;

  typedef char cache_line_pad_t[64];

  cache_line_pad_t _pad0;
  const size_t _size;
  const size_t _mask;
  T* const _buffer;
  cache_line_pad_t _pad1;
  size_t _head;
  cache_line_pad_t _pad2;
  size_t _tail;

  BoundedQueue(const BoundedQueue&) {}
  void operator=(const BoundedQueue&) {}
};

} // namespace cass

#endif
