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

#ifndef __CASS_ATOMIC_BASE_GCC_HPP_INCLUDED__
#define __CASS_ATOMIC_BASE_GCC_HPP_INCLUDED__

namespace cass {

enum MemoryOrder {
  MEMORY_ORDER_RELAXED,
  MEMORY_ORDER_CONSUME,
  MEMORY_ORDER_ACQUIRE,
  MEMORY_ORDER_RELEASE,
  MEMORY_ORDER_ACQ_REL,
  MEMORY_ORDER_SEQ_CST
};

template <class T>
class Atomic {
public:
  Atomic() {}
  explicit Atomic(T value) : value_(value) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
  }

  inline T fetch_add(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

  inline T fetch_sub(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

  inline bool compare_exchange_strong(T& expected, T desired, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

  inline bool compare_exchange_weak(T& expected, T desired, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
  }

private:
  T value_;
};

inline void atomic_thread_fence(MemoryOrder order) {
}

} // namespace cass

#endif
