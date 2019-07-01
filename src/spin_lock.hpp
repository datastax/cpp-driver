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

#include "atomic.hpp"
#include "macros.hpp"

#ifndef DATASTAX_INTERNAL_SPINLOCK_HPP
#define DATASTAX_INTERNAL_SPINLOCK_HPP

namespace datastax { namespace internal { namespace core {

class Spinlock {
public:
  typedef enum { LOCKED, UNLOCKED } LockState;

  Spinlock()
      : state_(UNLOCKED) {}

  void lock() {
    while (state_.exchange(LOCKED, MEMORY_ORDER_ACQUIRE) == LOCKED) {
      // Spin
    }
  }

  void unlock() { state_.store(UNLOCKED, MEMORY_ORDER_RELEASE); }

private:
  Atomic<LockState> state_;

  static const size_t cacheline_size = 64;
  char pad__[cacheline_size];
  void no_unused_private_warning__() { pad__[0] = 0; }
};

class ScopedSpinlock {
public:
  ScopedSpinlock(Spinlock* l, bool acquire_lock = true)
      : lock_(l)
      , is_locked_(false) {
    if (acquire_lock) {
      lock();
    }
  }

  ~ScopedSpinlock() {
    if (is_locked_) {
      unlock();
    }
  }

  void lock() {
    assert(!is_locked_);
    lock_->lock();
    is_locked_ = true;
  }

  void unlock() {
    assert(is_locked_);
    lock_->unlock();
    is_locked_ = false;
  }

private:
  mutable Spinlock* lock_;
  bool is_locked_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedSpinlock);
};

template <class N>
struct SpinlockPool {
  static Spinlock* get_spinlock(const void* p) {
    return &spinlocks_[reinterpret_cast<size_t>(p) % 41];
  }

private:
  static Spinlock spinlocks_[41];
};

template <class N>
Spinlock SpinlockPool<N>::spinlocks_[41];

}}} // namespace datastax::internal::core

#endif
