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

#include "ref_counted.hpp"

#ifndef DATASTAX_INTERNAL_COPY_ON_WRITE_PTR_HPP
#define DATASTAX_INTERNAL_COPY_ON_WRITE_PTR_HPP

#include <stdio.h>

namespace datastax { namespace internal { namespace core {

template <class T>
class CopyOnWritePtr {
public:
  CopyOnWritePtr(T* t)
      : ptr_(new Referenced(t)) {}

  CopyOnWritePtr(const SharedRefPtr<T>& shared)
      : ptr_(shared) {}

  CopyOnWritePtr(const CopyOnWritePtr<T>& cow)
      : ptr_(cow.ptr_) {}

  CopyOnWritePtr<T>& operator=(const SharedRefPtr<T>& rhs) {
    ptr_ = rhs;
    return *this;
  }

  CopyOnWritePtr<T>& operator=(const CopyOnWritePtr<T>& rhs) {
    ptr_ = rhs.ptr_;
    return *this;
  }

  operator bool() const { return ptr_->ref != NULL; }

  const T& operator*() const { return *(ptr_->ref); }

  T& operator*() {
    detach();
    return *(ptr_->ref);
  }

  const T* operator->() const { return ptr_->ref; }

  T* operator->() {
    detach();
    return ptr_->ref;
  }

private:
  void detach() {
    Referenced* temp = ptr_.get();
    if (temp->ref != NULL && temp->ref_count() > 1) {
      ptr_ = SharedRefPtr<Referenced>(new Referenced(new T(*(temp->ref))));
    }
  }

private:
  class Referenced : public RefCounted<Referenced> {
  public:
    Referenced(T* ref)
        : ref(ref) {}

    ~Referenced() { delete ref; }

    T* ref;
  };

  SharedRefPtr<Referenced> ptr_;
};

}}} // namespace datastax::internal::core

#endif
