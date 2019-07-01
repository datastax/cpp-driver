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

#ifndef DATASTAX_INTERNAL_ATOMIC_BOOST_HPP
#define DATASTAX_INTERNAL_ATOMIC_BOOST_HPP

#include <boost/atomic.hpp>

namespace datastax { namespace internal {

enum MemoryOrder {
  MEMORY_ORDER_RELAXED = boost::memory_order_relaxed,
  MEMORY_ORDER_CONSUME = boost::memory_order_consume,
  MEMORY_ORDER_ACQUIRE = boost::memory_order_acquire,
  MEMORY_ORDER_RELEASE = boost::memory_order_release,
  MEMORY_ORDER_ACQ_REL = boost::memory_order_acq_rel,
  MEMORY_ORDER_SEQ_CST = boost::memory_order_seq_cst
};

template <class T>
class Atomic {
public:
  Atomic() {}
  explicit Atomic(T value)
      : value_(value) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    value_.store(value, static_cast<boost::memory_order>(order));
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    return value_.load(static_cast<boost::memory_order>(order));
  }

  inline T fetch_add(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return value_.fetch_add(value, static_cast<boost::memory_order>(order));
  }

  inline T fetch_sub(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return value_.fetch_sub(value, static_cast<boost::memory_order>(order));
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return value_.exchange(value, static_cast<boost::memory_order>(order));
  }

  inline bool compare_exchange_strong(T& expected, T desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return value_.compare_exchange_strong(expected, desired,
                                          static_cast<boost::memory_order>(order));
  }

  inline bool compare_exchange_weak(T& expected, T desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return value_.compare_exchange_weak(expected, desired, static_cast<boost::memory_order>(order));
  }

private:
  boost::atomic<T> value_;
};

inline void atomic_thread_fence(MemoryOrder order) {
  boost::atomic_thread_fence(static_cast<boost::memory_order>(order));
}

}} // namespace datastax::internal

#endif
