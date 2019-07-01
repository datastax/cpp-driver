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

#ifndef DATASTAX_INTERNAL_SCOPED_PTR_HPP
#define DATASTAX_INTERNAL_SCOPED_PTR_HPP

#include "macros.hpp"
#include "utils.hpp"

#include <stddef.h>

namespace datastax { namespace internal {

template <class T>
struct DefaultDeleter {
  void operator()(T* ptr) const { delete ptr; }
};

template <class T, class D = DefaultDeleter<T> >
class ScopedPtr {
public:
  typedef T type;
  typedef D deleter;

  explicit ScopedPtr(type* ptr = NULL)
      : ptr_(ptr) {}

  ~ScopedPtr() { deleter()(ptr_); }

  void reset(type* ptr = NULL) {
    if (ptr_ != NULL) {
      deleter()(ptr_);
    }
    ptr_ = ptr;
  }

  type* release() {
    type* temp = ptr_;
    ptr_ = NULL;
    return temp;
  }

  type* get() const { return ptr_; }
  type& operator*() const { return *ptr_; }
  type* operator->() const { return ptr_; }
  operator bool() const { return ptr_ != NULL; }

private:
  type* ptr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedPtr);
};

template <class T>
struct DefaultArrayDeleter {
  void operator()(T* ptr) const { delete[] ptr; }
};

template <class T, class D = DefaultArrayDeleter<T> >
class ScopedArray {
public:
  typedef T type;
  typedef D deleter;

  explicit ScopedArray(type* ptr = NULL)
      : ptr_(ptr) {}

  ~ScopedArray() { deleter()(ptr_); }

  void reset(type* ptr = NULL) {
    if (ptr_ != NULL) {
      deleter()(ptr_);
    }
    ptr_ = ptr;
  }

  type* release() {
    type* temp = ptr_;
    ptr_ = NULL;
    return temp;
  }

  type* get() const { return ptr_; }
  type& operator[](size_t index) const { return ptr_[index]; }
  operator bool() const { return ptr_ != NULL; }

private:
  type* ptr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ScopedArray);
};

}} // namespace datastax::internal

#endif
