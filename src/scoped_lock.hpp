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

#ifndef DATASTAX_INTERNAL_SCOPED_LOCK_HPP
#define DATASTAX_INTERNAL_SCOPED_LOCK_HPP

#include "macros.hpp"

#include <assert.h>
#include <uv.h>

namespace datastax { namespace internal {

class Mutex {
public:
  typedef uv_mutex_t Type;
  Mutex(uv_mutex_t* m)
      : mutex_(m) {}
  void lock() { uv_mutex_lock(mutex_); }
  void unlock() { uv_mutex_unlock(mutex_); }
  uv_mutex_t* get() const { return mutex_; }

private:
  uv_mutex_t* mutex_;
};

class ReadLock {
public:
  typedef uv_rwlock_t Type;
  ReadLock(uv_rwlock_t* l)
      : rwlock_(l) {}
  void lock() { uv_rwlock_rdlock(rwlock_); }
  void unlock() { uv_rwlock_rdunlock(rwlock_); }
  uv_rwlock_t* get() const { return rwlock_; }

private:
  uv_rwlock_t* rwlock_;
};

class WriteLock {
public:
  typedef uv_rwlock_t Type;
  WriteLock(uv_rwlock_t* l)
      : rwlock_(l) {}
  void lock() { uv_rwlock_wrlock(rwlock_); }
  void unlock() { uv_rwlock_wrunlock(rwlock_); }
  uv_rwlock_t* get() const { return rwlock_; }

private:
  uv_rwlock_t* rwlock_;
};

template <class Lock>
class ScopedLock {
public:
  ScopedLock(typename Lock::Type* l, bool acquire_lock = true)
      : lock_(l)
      , is_locked_(false) {
    if (acquire_lock) {
      lock();
    }
  }

  ~ScopedLock() {
    if (is_locked_) {
      unlock();
    }
  }

  typename Lock::Type* get() const { return lock_.get(); }

  void lock() {
    assert(!is_locked_);
    lock_.lock();
    is_locked_ = true;
  }

  void unlock() {
    assert(is_locked_);
    lock_.unlock();
    is_locked_ = false;
  }

private:
  mutable Lock lock_;
  bool is_locked_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedLock);
};

typedef ScopedLock<Mutex> ScopedMutex;
typedef ScopedLock<ReadLock> ScopedReadLock;
typedef ScopedLock<WriteLock> ScopedWriteLock;

}} // namespace datastax::internal

#endif
