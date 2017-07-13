/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __SHARED_PTR_HPP__
#define __SHARED_PTR_HPP__
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
 * Shared pointer for object references
 */
template<typename T, class D = cass::DefaultDeleter<T> >
class SharedPtr {
public:
  SharedPtr(T* ptr = NULL)
    : object_(NULL) {
    if (ptr) {
      ObjectRef<T, D>* object_ref = new ObjectRef<T, D>(ptr);
      object_ = cass::SharedRefPtr<ObjectRef<T, D> >(object_ref);
    }
  }

  T* operator->() {
    return get();
  }

  T& operator*() {
    return *get();
  }

  operator bool() const {
    return object_;
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
  cass::SharedRefPtr<ObjectRef<T, D> > object_;
};

#endif // __SHARED_PTR_HPP__
