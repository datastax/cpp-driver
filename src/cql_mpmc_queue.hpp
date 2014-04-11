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

/*
  Implementation of Dmitry Vyukov's MPMC algorithm
  http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
*/

#ifndef __CQL_MPMC_QUEUE_INCLUDED__
#define __CQL_MPMC_QUEUE_INCLUDED__

#include <atomic>
#include <assert.h>

namespace cql {

template<typename T>
class MpmcQueue {
 public:
  MpmcQueue(
      size_t size) :
      size_(size),
      mask_(size - 1),
      buffer_(reinterpret_cast<Node*>(new AlignedNode[size_])),
      head_seq_(0),
      tail_seq_(0) {
    // make sure it's a power of 2
    assert((size_ != 0) && ((size_ & (~size_ + 1)) == size_));

    // populate the sequence initial values
    for (size_t i = 0; i < size_; ++i) {
      buffer_[i].seq.store(i, std::memory_order_relaxed);
    }
  }

  ~MpmcQueue() {
    delete[] buffer_;
  }

  bool
  enqueue(
      const T& data) {
    // head_seq_ only wraps at MAX(head_seq_) instead we use a mask to
    // convert the sequence to an array index this is why the ring
    // buffer must be a size which is a power of 2. this also allows
    // the sequence to double as a ticket/lock.
    size_t  head_seq = head_seq_.load(std::memory_order_relaxed);

    for (;;) {
      Node*    node     = &buffer_[head_seq & mask_];
      size_t   node_seq = node->seq.load(std::memory_order_acquire);
      intptr_t dif      = (intptr_t) node_seq - (intptr_t) head_seq;

      // if seq and head_seq are the same then it means this slot is empty
      if (dif == 0) {
        // claim our spot by moving head if head isn't the same as we
        // last checked then that means someone beat us to the punch
        // weak compare is faster, but can return spurious results
        // which in this instance is OK, because it's in the loop
        if (head_seq_.compare_exchange_weak(
                head_seq,
                head_seq + 1,
                std::memory_order_relaxed)) {
          // set the data
          node->data = data;
          // increment the sequence so that the tail knows it's accessible
          node->seq.store(head_seq + 1, std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        // if seq is less than head seq then it means this slot is
        // full and therefore the buffer is full
        return false;
      } else {
        // under normal circumstances this branch should never be taken
        head_seq = head_seq_.load(std::memory_order_relaxed);
      }
    }

    // never taken
    return false;
  }

  bool
  dequeue(
      T& data) {
    size_t       tail_seq = tail_seq_.load(std::memory_order_relaxed);

    for (;;) {
      Node*    node     = &buffer_[tail_seq & mask_];
      size_t   node_seq = node->seq.load(std::memory_order_acquire);
      intptr_t dif      = (intptr_t) node_seq - (intptr_t)(tail_seq + 1);

      // if seq and head_seq are the same then it means this slot is empty
      if (dif == 0) {
        // claim our spot by moving head if head isn't the same as we
        // last checked then that means someone beat us to the punch
        // weak compare is faster, but can return spurious results
        // which in this instance is OK, because it's in the loop
        if (tail_seq_.compare_exchange_weak(
                tail_seq,
                tail_seq + 1,
                std::memory_order_relaxed)) {
          // set the output
          data = node->data;
          // set the sequence to what the head sequence should be next
          // time around
          node->seq.store(tail_seq + mask_ + 1, std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        // if seq is less than head seq then it means this slot is
        // full and therefore the buffer is full
        return false;
      } else {
        // under normal circumstances this branch should never be taken
        tail_seq = tail_seq_.load(std::memory_order_relaxed);
      }
    }

    // never taken
    return false;
  }

 private:
  struct Node {
    T                     data;
    std::atomic<size_t>   seq;
  };

  typedef typename std::aligned_storage<
    sizeof(Node),
    std::alignment_of<Node>::value>::type
  AlignedNode;

  // it's either 32 or 64 so 64 is good enough
  typedef char CachePad[64];

  CachePad            pad0_;
  const size_t        size_;
  const size_t        mask_;
  Node* const         buffer_;
  CachePad            pad1_;
  std::atomic<size_t> head_seq_;
  CachePad            pad2_;
  std::atomic<size_t> tail_seq_;
  CachePad            pad3_;

  MpmcQueue(const MpmcQueue&) {}
  void operator=(const MpmcQueue&) {}
};
}
#endif
