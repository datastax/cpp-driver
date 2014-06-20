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

#ifndef __CASS_SCOPED_MUTEX_HPP_INCLUDED__
#define __CASS_SCOPED_MUTEX_HPP_INCLUDED__

#include "common.hpp"

#include <uv.h>
#include <assert.h>

namespace cass {

class ScopedMutex {
public:
  ScopedMutex(uv_mutex_t* mutex, bool acquire_lock = true)
      : mutex_(mutex)
      , is_locked_(false) {
    if(acquire_lock) {
      lock();
    }
  }

  ~ScopedMutex() {
    if(is_locked_) {
      unlock();
    }
  }

  uv_mutex_t* get() const {
    return mutex_;
  }

  void lock() {
    assert(!is_locked_);
    uv_mutex_lock(mutex_);
    is_locked_ = true;
  }

  void unlock() {
    assert(is_locked_);
    uv_mutex_unlock(mutex_);
    is_locked_ = false;
  }

private:
  uv_mutex_t* mutex_;
  bool is_locked_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedMutex);
};

} // namespace cass

#endif
