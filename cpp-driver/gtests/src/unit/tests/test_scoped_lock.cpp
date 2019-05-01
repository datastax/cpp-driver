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

#include <gtest/gtest.h>

#include "scoped_lock.hpp"

using datastax::internal::ScopedMutex;
using datastax::internal::ScopedReadLock;
using datastax::internal::ScopedWriteLock;

struct MutexData {
  MutexData(uv_mutex_t* mutex)
      : mutex(mutex)
      , error_code(0) {}

  uv_mutex_t* mutex;
  int error_code;
};

struct RwlockData {
  RwlockData(uv_rwlock_t* rwlock)
      : rwlock(rwlock)
      , error_code(0) {}

  uv_rwlock_t* rwlock;
  int error_code;
};

void on_mutex_trylock(void* arg) {
  MutexData* mutex_data = static_cast<MutexData*>(arg);
  mutex_data->error_code = uv_mutex_trylock(mutex_data->mutex);
  if (mutex_data->error_code == 0) uv_mutex_unlock(mutex_data->mutex);
}

void on_rwlock_tryrdlock(void* arg) {
  RwlockData* rwlock_data = static_cast<RwlockData*>(arg);
  rwlock_data->error_code = uv_rwlock_tryrdlock(rwlock_data->rwlock);
  if (rwlock_data->error_code == 0) uv_rwlock_rdunlock(rwlock_data->rwlock);
}

void on_rwlock_trywrlock(void* arg) {
  RwlockData* rwlock_data = static_cast<RwlockData*>(arg);
  rwlock_data->error_code = uv_rwlock_trywrlock(rwlock_data->rwlock);
  if (rwlock_data->error_code == 0) uv_rwlock_wrunlock(rwlock_data->rwlock);
}

TEST(ScopedLockUnitTest, ScopedMutex) {
  uv_mutex_t mutex;
  uv_mutex_init(&mutex);
  MutexData mutex_data(&mutex);

  {
    ScopedMutex lock(&mutex);

    uv_thread_t thread;
    uv_thread_create(&thread, on_mutex_trylock, &mutex_data);
    uv_thread_join(&thread);
  }

  ASSERT_NE(0, mutex_data.error_code);
  uv_mutex_destroy(&mutex);
}

TEST(ScopedLockUnitTest, ScopedMutexDefaultUnlocked) {
  uv_mutex_t mutex;
  uv_mutex_init(&mutex);
  MutexData mutex_data(&mutex);

  {
    ScopedMutex lock(&mutex, false);

    uv_thread_t thread;
    uv_thread_create(&thread, on_mutex_trylock, &mutex_data);
    uv_thread_join(&thread);
  }

  ASSERT_EQ(0, mutex_data.error_code); // Lock would have been obtained
  uv_mutex_destroy(&mutex);
}

TEST(ScopedLockUnitTest, ScopedReadLock) {
  uv_rwlock_t rwlock;
  uv_rwlock_init(&rwlock);
  RwlockData rwlock_data(&rwlock);

  {
    ScopedReadLock rl(&rwlock);

    uv_thread_t thread;
    uv_thread_create(&thread, on_rwlock_trywrlock,
                     &rwlock_data); // Lock for writing to force busy for write
    uv_thread_join(&thread);
  }

  ASSERT_EQ(UV_EBUSY, rwlock_data.error_code); // Lock would not have been obtained
  uv_rwlock_destroy(&rwlock);
}

TEST(ScopedLockUnitTest, ScopedReadLockDefaultUnlocked) {
  uv_rwlock_t rwlock;
  uv_rwlock_init(&rwlock);
  RwlockData rwlock_data(&rwlock);

  {
    ScopedReadLock rl(&rwlock, false);

    uv_thread_t thread;
    uv_thread_create(&thread, on_rwlock_trywrlock, &rwlock_data);
    uv_thread_join(&thread);
  }

  ASSERT_EQ(0, rwlock_data.error_code); // Lock would have been obtained
  uv_rwlock_destroy(&rwlock);
}

TEST(ScopedLockUnitTest, ScopedWriteLock) {
  uv_rwlock_t rwlock;
  uv_rwlock_init(&rwlock);
  RwlockData rwlock_data(&rwlock);

  {
    ScopedWriteLock wl(&rwlock);

    uv_thread_t thread;
    uv_thread_create(&thread, on_rwlock_trywrlock, &rwlock_data);
    uv_thread_join(&thread);
  }

  ASSERT_NE(0, rwlock_data.error_code);
  uv_rwlock_destroy(&rwlock);
}

TEST(ScopedLockUnitTest, ScopedWriteLockDefaultUnlocked) {
  uv_rwlock_t rwlock;
  uv_rwlock_init(&rwlock);
  RwlockData rwlock_data(&rwlock);

  {
    ScopedWriteLock wl(&rwlock, false);

    uv_thread_t thread;
    uv_thread_create(&thread, on_rwlock_trywrlock, &rwlock_data);
    uv_thread_join(&thread);
  }

  ASSERT_EQ(0, rwlock_data.error_code); // Lock would have been obtained
  uv_rwlock_destroy(&rwlock);
}

TEST(ScopedLockUnitTest, ScopedWriteLockBusy) {
  uv_rwlock_t rwlock;
  uv_rwlock_init(&rwlock);
  RwlockData rwlock_data(&rwlock);

  {
    ScopedWriteLock wl(&rwlock);

    uv_thread_t thread;
    uv_thread_create(&thread, on_rwlock_tryrdlock,
                     &rwlock_data); // Lock for reading to force busy for read
    uv_thread_join(&thread);
  }

  ASSERT_EQ(UV_EBUSY, rwlock_data.error_code);
  uv_rwlock_destroy(&rwlock);
}
