/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_ATOMIC_STD_HPP_INCLUDED__
#define __CASS_ATOMIC_STD_HPP_INCLUDED__

#include <atomic>

namespace cass {

enum MemoryOrder {
  MEMORY_ORDER_RELAXED = std::memory_order_relaxed,
  MEMORY_ORDER_CONSUME = std::memory_order_consume,
  MEMORY_ORDER_ACQUIRE = std::memory_order_acquire,
  MEMORY_ORDER_RELEASE = std::memory_order_release,
  MEMORY_ORDER_ACQ_REL = std::memory_order_acq_rel,
  MEMORY_ORDER_SEQ_CST = std::memory_order_seq_cst
};

template <class T>
class Atomic {
public:
  Atomic() {}
  explicit Atomic(T value) : value_(value) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    value_.store(value, static_cast<std::memory_order>(order));
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const {
    return value_.load(static_cast<std::memory_order>(order));
  }

  inline T fetch_add(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    return value_.fetch_add(value, static_cast<std::memory_order>(order));
  }

  inline T fetch_sub(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    return value_.fetch_sub(value, static_cast<std::memory_order>(order));
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    return value_.exchange(value, static_cast<std::memory_order>(order));
  }

  inline bool compare_exchange_strong(T& expected, T desired, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    return value_.compare_exchange_strong(expected, desired, static_cast<std::memory_order>(order));
  }

  inline bool compare_exchange_weak(T& expected, T desired, MemoryOrder order = MEMORY_ORDER_SEQ_CST) {
    return value_.compare_exchange_weak(expected, desired, static_cast<std::memory_order>(order));
  }

private:
  std::atomic<T> value_;
};

inline void atomic_thread_fence(MemoryOrder order) {
  std::atomic_thread_fence(static_cast<std::memory_order>(order));
}

} // namespace cass

#endif
