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
*/

/*
  Implementation of Dmitry Vyukov's MPMC algorithm
  http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
*/

#ifndef __CASS_MPMC_QUEUE_INCLUDED__
#define __CASS_MPMC_QUEUE_INCLUDED__

#include "atomic.hpp"
#include "utils.hpp"
#include "macros.hpp"

#include <assert.h>

namespace cass {

template <typename T>
class MPMCQueue {
public:
  typedef T EntryType;

  MPMCQueue(size_t size)
      : size_(next_pow_2(size))
      , mask_(size_ - 1)
      , buffer_(new Node[size_])
      , tail_(0)
      , head_(0) {
    // populate the sequence initial values
    for (size_t i = 0; i < size_; ++i) {
      buffer_[i].seq.store(i, MEMORY_ORDER_RELAXED);
    }
  }

  ~MPMCQueue() { delete[] buffer_; }

  bool enqueue(const T& data) {
    // head_seq_ only wraps at MAX(head_seq_) instead we use a mask to
    // convert the sequence to an array index this is why the ring
    // buffer must be a size which is a power of 2. this also allows
    // the sequence to double as a ticket/lock.
    size_t pos = tail_.load(MEMORY_ORDER_RELAXED);

    for (;;) {
      Node* node = &buffer_[pos & mask_];
      size_t node_seq = node->seq.load(MEMORY_ORDER_ACQUIRE);
      intptr_t dif = (intptr_t)node_seq - (intptr_t)pos;

      // if seq and head_seq are the same then it means this slot is empty
      if (dif == 0) {
        // claim our spot by moving head if head isn't the same as we
        // last checked then that means someone beat us to the punch
        // weak compare is faster, but can return spurious results
        // which in this instance is OK, because it's in the loop
        if (tail_.compare_exchange_weak(pos, pos + 1, MEMORY_ORDER_RELAXED)) {
          // set the data
          node->data = data;
          // increment the sequence so that the tail knows it's accessible
          node->seq.store(pos + 1, MEMORY_ORDER_RELEASE);
          return true;
        }
      } else if (dif < 0) {
        // if seq is less than head seq then it means this slot is
        // full and therefore the buffer is full
        return false;
      } else {
        // under normal circumstances this branch should never be taken
        pos = tail_.load(MEMORY_ORDER_RELAXED);
      }
    }

    // never taken
    return false;
  }

  bool dequeue(T& data) {
    size_t pos = head_.load(MEMORY_ORDER_RELAXED);

    for (;;) {
      Node* node = &buffer_[pos & mask_];
      size_t node_seq = node->seq.load(MEMORY_ORDER_ACQUIRE);
      intptr_t dif = (intptr_t)node_seq - (intptr_t)(pos + 1);

      // if seq and head_seq are the same then it means this slot is empty
      if (dif == 0) {
        // claim our spot by moving head if head isn't the same as we
        // last checked then that means someone beat us to the punch
        // weak compare is faster, but can return spurious results
        // which in this instance is OK, because it's in the loop
        if (head_.compare_exchange_weak(pos, pos + 1, MEMORY_ORDER_RELAXED)) {
          // set the output
          data = node->data;
          // set the sequence to what the head sequence should be next
          // time around
          node->seq.store(pos + mask_ + 1, MEMORY_ORDER_RELEASE);
          return true;
        }
      } else if (dif < 0) {
        // if seq is less than head seq then it means this slot is
        // full and therefore the buffer is full
        return false;
      } else {
        // under normal circumstances this branch should never be taken
        pos = head_.load(MEMORY_ORDER_RELAXED);
      }
    }

    // never taken
    return false;
  }

  bool is_empty() const {
    size_t pos = head_.load(MEMORY_ORDER_ACQUIRE);
    Node* node = &buffer_[pos & mask_];
    size_t node_seq = node->seq.load(MEMORY_ORDER_ACQUIRE);
    return (intptr_t)node_seq - (intptr_t)(pos + 1) < 0;
  }

  static void memory_fence() {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
    // No fence required becauase compare_exchange_weak() emits "lock cmpxchg" on x86/x64 enforcing total order.
#elif defined(CASS_USE_BOOST_ATOMIC) || defined(CASS_USE_STD_ATOMIC)
    atomic_thread_fence(MEMORY_ORDER_SEQ_CST);
#endif
  }

private:
  struct Node {
    Atomic<size_t> seq;
    T data;
  };

  // it's either 32 or 64 so 64 is good enough
  typedef char CachePad[64];

  CachePad pad0_;
  const size_t size_;
  const size_t mask_;
  Node* const buffer_;
  CachePad pad1_;
  Atomic<size_t> tail_;
  CachePad pad2_;
  Atomic<size_t> head_;
  CachePad pad3_;

  DISALLOW_COPY_AND_ASSIGN(MPMCQueue);
};

} // namespace cass

#endif
