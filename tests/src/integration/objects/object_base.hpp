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

#ifndef __TEST_OBJECT_BASE_HPP__
#define __TEST_OBJECT_BASE_HPP__
#include "exception.hpp"
#include "shared_ptr.hpp"
#include "tlog.hpp"

namespace test { namespace driver {

/**
 * Template deleter class for driver objects that utilize a freeing function
 */
template <class T, void (*free_function)(T*)>
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
template <class T, void (*free_function)(T*)>
class Object {
public:
  typedef Deleter<T, free_function> Dtor;
  typedef SharedPtr<T, Dtor> Ptr;

  /**
   * Create an empty object
   */
  Object()
      : object_(NULL) {}

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
  operator bool() const { return object_.get() != NULL; }

  /**
   * Conversion operator for determining if the pointer used for the object is
   * valid
   *
   * @return True if object pointer is valid; false otherwise
   */
  operator bool() { return object_.get() != NULL; }

protected:
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
  void set(T* object) { object_ = object; }

private:
  /**
   * Object reference
   */
  Ptr object_;
};

}} // namespace test::driver

#endif // __TEST_OBJECT_BASE_HPP__
