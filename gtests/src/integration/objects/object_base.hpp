/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_OBJECT_BASE_HPP__
#define __TEST_OBJECT_BASE_HPP__
#include "cassandra.h"

#include "exception.hpp"
#include "shared_ptr.hpp"

namespace test {
namespace driver {

/**
 * Template deleter class for driver objects that utilize a freeing function
 */
template<class T, void (*free_function)(T*)>
class Deleter {
public:
  void operator()(T* object) {
    if (object) {
      free_function(object);
    }
  }
};

/**
 * Template object class for driver objects
 */
template<class T, void (*free_function)(T*)>
class Object {
public:
  typedef Deleter<T, free_function> Dtor;
  typedef SharedPtr<T, Dtor> Ptr;

  /**
   * Create the object with the native pointer object
   *
   * @param object Native pointer object
   */
  Object(T* object)
    : object_(object) {}

  /**
   * Create the object with the reference pointer object
   *
   * @param object Shared reference pointer object
   */
  Object(Ptr object)
    : object_(object) {}

  /**
   * Get the object pointer
   *
   * @return Object pointer
   * @throws test::Exception Use of null object
   */
  T* get() {
    check_null();
    return object_.get();
  }

  /**
   * Get the object pointer
   *
   * @return Object pointer
   * @throws test::Exception Use of null object
   */
  const T* get() const {
    check_null();
    return object_.get();
  }

  /**
   * Conversion operator for determining if the pointer used for the object is
   * valid
   *
   * @return True if object pointer is valid; false otherwise
   */
  operator bool() const {
    return object_.get() != NULL;
  }

  /**
   * Conversion operator for determining if the pointer used for the object is
   * valid
   *
   * @return True if object pointer is valid; false otherwise
   */
  operator bool() {
    return object_.get() != NULL;
  }

protected:
  /**
   * Create an empty object
   */
  Object() {}

  /**
   * Check to see if the object is NULL
   *
   * @throws test::Exception Object is NULL
   */
  void check_null() const {
    if (!object_) {
      throw Exception("Attempted to use null object");
    }
  }

  /**
   * Assign the object reference
   *
   * @param object Object reference
   */
  void set(T* object) {
    object_ = object;
  }

private:
  /**
   * Object reference
   */
  Ptr object_;
};

} // namespace driver
} // namespace test

#endif // __TEST_OBJECT_BASE_HPP__
