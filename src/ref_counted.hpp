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

#ifndef DATASTAX_INTERNAL_REF_COUNTED_HPP
#define DATASTAX_INTERNAL_REF_COUNTED_HPP

#include "allocated.hpp"
#include "atomic.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <assert.h>
#include <new>
#include <uv.h>

namespace datastax { namespace internal {

template <class T>
class RefCounted : public Allocated {
public:
  RefCounted()
      : ref_count_(0) {}

  int ref_count() const { return ref_count_.load(MEMORY_ORDER_ACQUIRE); }

  void inc_ref() const { ref_count_.fetch_add(1, MEMORY_ORDER_RELAXED); }

  void dec_ref() const {
    int new_ref_count = ref_count_.fetch_sub(1, MEMORY_ORDER_RELEASE);
    assert(new_ref_count >= 1);
    if (new_ref_count == 1) {
      atomic_thread_fence(MEMORY_ORDER_ACQUIRE);
#ifdef THREAD_SANITIZER
      __tsan_acquire(const_cast<void*>(static_cast<const void*>(this)));
#endif
      delete static_cast<const T*>(this);
    }
  }

private:
  mutable Atomic<int> ref_count_;
  DISALLOW_COPY_AND_ASSIGN(RefCounted);
};

template <class T>
class SharedRefPtr {
public:
  explicit SharedRefPtr(T* ptr = NULL)
      : ptr_(ptr) {
    if (ptr_ != NULL) {
      ptr_->inc_ref();
    }
  }

  SharedRefPtr(const SharedRefPtr<T>& ref)
      : ptr_(NULL) {
    copy<T>(ref.ptr_);
  }

  template <class S>
  SharedRefPtr(const SharedRefPtr<S>& ref)
      : ptr_(NULL) {
    copy<S>(ref.ptr_);
  }

  SharedRefPtr<T>& operator=(const SharedRefPtr<T>& ref) {
    copy<T>(ref.ptr_);
    return *this;
  }

  template <class S>
  SharedRefPtr<S>& operator=(const SharedRefPtr<S>& ref) {
    copy<S>(ref.ptr_);
    return *this;
  }

#if defined(__cpp_rvalue_references)
  SharedRefPtr<T>& operator=(SharedRefPtr<T>&& ref) noexcept {
    if (ptr_ != NULL) {
      ptr_->dec_ref();
    }
    ptr_ = ref.ptr_;
    ref.ptr_ = NULL;
    return *this;
  }

  SharedRefPtr(SharedRefPtr<T>&& ref) noexcept : ptr_(ref.ptr_) { ref.ptr_ = NULL; }
#endif

  ~SharedRefPtr() {
    if (ptr_ != NULL) {
      ptr_->dec_ref();
    }
  }

  bool operator==(const T* ptr) const { return ptr_ == ptr; }

  bool operator==(const SharedRefPtr<T>& ref) const { return ptr_ == ref.ptr_; }

  void reset(T* ptr = NULL) { copy<T>(ptr); }

  T* get() const { return ptr_; }
  T& operator*() const { return *ptr_; }
  T* operator->() const { return ptr_; }
  operator bool() const { return ptr_ != NULL; }

private:
  template <class S>
  friend class SharedRefPtr;

  template <class S>
  void copy(S* ptr) {
    if (ptr == ptr_) return;
    if (ptr != NULL) {
      ptr->inc_ref();
    }
    T* temp = ptr_;
    ptr_ = static_cast<T*>(ptr);
    if (temp != NULL) {
      temp->dec_ref();
    }
  }

  T* ptr_;
};

class RefBuffer : public RefCounted<RefBuffer> {
public:
  typedef SharedRefPtr<RefBuffer> Ptr;

  static RefBuffer* create(size_t size) {
#if defined(_WIN32)
#pragma warning(push)
#pragma warning(disable : 4291) // Invalid warning thrown RefBuffer has a delete function
#endif
    return new (size) RefBuffer();
#if defined(_WIN32)
#pragma warning(pop)
#endif
  }

  char* data() { return reinterpret_cast<char*>(this) + sizeof(RefBuffer); }

  void operator delete(void* ptr) { Memory::free(ptr); }

private:
  RefBuffer() {}

  void* operator new(size_t size, size_t extra) { return Memory::malloc(size + extra); }

  DISALLOW_COPY_AND_ASSIGN(RefBuffer);
};

}} // namespace datastax::internal

#endif
