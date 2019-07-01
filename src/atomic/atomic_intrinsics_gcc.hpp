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

// This implementation is based of the implementation in Boost:
// "boost/atomic/detail/ops_gcc_sync.hpp". Here is the license
// and copyright from that file:

/*
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Copyright (c) 2009 Helge Bahmann
 * Copyright (c) 2012 Tim Blechmann
 * Copyright (c) 2014 Andrey Semashev
 */

#ifndef DATASTAX_INTERNAL_ATOMIC_INTRINSICS_GCC_HPP
#define DATASTAX_INTERNAL_ATOMIC_INTRINSICS_GCC_HPP

#include <assert.h>

namespace datastax { namespace internal {

template <class T>
class Atomic {
public:
  Atomic() {}
  explicit Atomic(T value)
      : value_(value) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    assert(order != MEMORY_ORDER_ACQUIRE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    if ((order & MEMORY_ORDER_RELEASE) != 0) {
      __sync_synchronize();
    }
    value_ = value;
    if (order == MEMORY_ORDER_SEQ_CST) {
      __sync_synchronize();
    }
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    assert(order != MEMORY_ORDER_RELEASE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    T value = value_;
    if ((order & (MEMORY_ORDER_ACQUIRE | MEMORY_ORDER_CONSUME)) != 0) {
      __sync_synchronize();
    }
    return value;
  }

  inline T fetch_add(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return __sync_fetch_and_add(&value_, value);
  }

  inline T fetch_sub(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return __sync_fetch_and_sub(&value_, value);
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
#if defined(__x86_64__) || defined(__i386__) // See below
    // The atomic exchange below only has MEMORY_ORDER_ACQUIRE semantics
    if ((order & MEMORY_ORDER_RELEASE) != 0) {
      __sync_synchronize();
    }
    // According to GCC this may only support storing the immediate value of 1 on some
    // platforms, but is an atomic exchange operation on Intel processors.
    return __sync_lock_test_and_set(&value_, value);
#else
    T before = load(MEMORY_ORDER_RELAXED);
    while (!compare_exchange_weak(before, before, order)) {
    }
    return before;
#endif
  }
  inline bool compare_exchange_strong(T& expected, T desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    T temp_expected = expected;
    T previous = __sync_val_compare_and_swap(&value_, temp_expected, desired);
    expected = previous;
    return previous == temp_expected;
  }

  inline bool compare_exchange_weak(T& expected, T desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return compare_exchange_strong(expected, desired, order);
  }

private:
  T value_;
};

inline void atomic_thread_fence(MemoryOrder order) {
  if (order != MEMORY_ORDER_RELAXED) {
    __sync_synchronize();
  }
}

}} // namespace datastax::internal

#endif
