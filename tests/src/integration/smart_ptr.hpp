/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __SMART_PTR_HPP__
#define __SMART_PTR_HPP__
#include "scoped_ptr.hpp"
#include "ref_counted.hpp"

/**
 * Reference counted objects container
 */
template<typename T, class D = cass::DefaultDeleter<T> >
class ObjectRef : public cass::RefCounted< ObjectRef<T, D> > {
public:
  ObjectRef(T* ptr)
    : ptr_(ptr) {}

  virtual ~ObjectRef() {
    D()(ptr_);
  }

  /**
   * Get the native object
   *
   * @return The native object
   */
  T* get() {
    return ptr_;
  }

private:
  /**
   * Native object
   */
  T* ptr_;
};

/**
 * Smart pointer for object references
 */
template<typename T, class D = cass::DefaultDeleter<T> >
class SmartPtr {
public:
  SmartPtr(T* ptr = NULL)
    : object_(NULL) {
    if (ptr) {
      // Create the reference object
      object_ = new ObjectRef<T, D>(ptr);
      object_->inc_ref();
    }
  }

  ~SmartPtr() {
    if (object_) {
      object_->dec_ref();
    }
  }

  SmartPtr(const SmartPtr& object)
    : object_(object.object_) {
    if (object_) {
      object_->inc_ref();
    }
  }

  SmartPtr& operator=(const SmartPtr& object) {
    ObjectRef<T,D>* const old = object_;
    object_ = object.object_;
    if (object_) {
      object_->inc_ref();
    }
    if (old) {
      old->dec_ref();
    }
    return *this;
  }

  T* operator->() {
    return get();
  }

  T& operator*() {
    return *get();
  }

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

private:
  /**
   * Object reference
   */
  ObjectRef<T, D>* object_;
};

#endif // __SMART_PTR_HPP__
