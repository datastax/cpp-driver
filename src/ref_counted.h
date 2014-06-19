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

#ifndef __CASS_REF_COUNTED_HPP_INCLUDED__
#define __CASS_REF_COUNTED_HPP_INCLUDED__

#include "third_party/boost/boost/atomic.hpp"
#include "scoped_ptr.hpp"

namespace cass {

template <class T>
class RefCounted {
public:
  RefCounted()
      : ref_count_(1) {}

  void retain() { ++ref_count_; }

  void release() {
    int new_ref_count = --ref_count_;
    assert(new_ref_count >= 0);
    if (new_ref_count == 0) {
       delete static_cast<T*>(this);
    }
  }

private:
  boost::atomic<int> ref_count_;
  DISALLOW_COPY_AND_ASSIGN(RefCounted);
};

template<class T>
class ScopedRefPtr {
public:
  typedef T type;

  explicit ScopedRefPtr(type* ptr = NULL)
       : ptr_(ptr) {
    if(ptr_ != NULL) {
      ptr_->retain();
    }
  }

  ~ScopedRefPtr() {
    if(ptr_ != NULL) {
      ptr_->release();
    }
  }

  void reset(type* ptr = NULL) {
    if(ptr != NULL) {
      ptr->retain();
    }
    type* temp = ptr_;
    ptr_ = ptr;
    if(temp != NULL) {
      temp->release();
    }
  }

  type* get() const { return ptr_; }
  type& operator*() const { return *ptr_; }
  type* operator->() const { return ptr_; }
  operator bool() const { return ptr_ != NULL; }

private:
  type* ptr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedRefPtr);
};

} // namespace cass

#endif
