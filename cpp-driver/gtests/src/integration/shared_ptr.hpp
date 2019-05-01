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

#ifndef __SHARED_PTR_HPP__
#define __SHARED_PTR_HPP__
#include "ref_counted.hpp"
#include "scoped_ptr.hpp"

template <class T>
struct StdDeleter {
  void operator()(T* ptr) const { delete ptr; }
};

/**
 * Reference counted objects container
 */
template <typename T, class D = datastax::internal::DefaultDeleter<T> >
class ObjectRef : public datastax::internal::RefCounted<ObjectRef<T, D> > {
public:
  ObjectRef(T* ptr)
      : ptr_(ptr) {}

  virtual ~ObjectRef() { D()(ptr_); }

  /**
   * Get the native object
   *
   * @return The native object
   */
  T* get() { return ptr_; }

private:
  /**
   * Native object
   */
  T* ptr_;
};

/**
 * Shared pointer for object references
 */
template <typename T, class D = datastax::internal::DefaultDeleter<T> >
class SharedPtr {
public:
  SharedPtr(T* ptr = NULL)
      : object_(NULL) {
    if (ptr) {
      ObjectRef<T, D>* object_ref = new ObjectRef<T, D>(ptr);
      object_ = datastax::internal::SharedRefPtr<ObjectRef<T, D> >(object_ref);
    }
  }

  T* operator->() { return get(); }

  T& operator*() { return *get(); }

  operator bool() const { return object_; }

  /**
   * Get the native object from the object reference
   *
   * @return The object
   */
  T* get() {
    if (object_) {
      return object_->get();
    }
    return NULL;
  }

  const T* get() const {
    if (object_) {
      return object_->get();
    }
    return NULL;
  }

private:
  /**
   * Object reference
   */
  datastax::internal::SharedRefPtr<ObjectRef<T, D> > object_;
};

#endif // __SHARED_PTR_HPP__
