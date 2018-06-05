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

#ifndef __CASS_CALLBACK_HPP_INCLUDED__
#define __CASS_CALLBACK_HPP_INCLUDED__

#include "aligned_storage.hpp"
#include "macros.hpp"

#include <new>
#include <stddef.h>

namespace cass {

template <class R, class Arg, size_t MaxSize = 4 * sizeof(void*)>
class Callback {
public:
  struct FunctionWithDataDummy { };

  Callback()
    : invoker_(NULL) { }

  template <class F, class T>
  Callback(F func, T* object)
    : invoker_(new (&storage_) MemberInvoker<F, T>(func, object)) {
    typedef MemberInvoker<F, T> MemberInvoker;
    STATIC_ASSERT(sizeof(Storage) >= sizeof(MemberInvoker));
    STATIC_ASSERT(ALIGN_OF(Storage) >= ALIGN_OF(MemberInvoker));
  }

  template <class F>
  explicit Callback(F func)
    : invoker_(new (&storage_) FunctionInvoker<F>(func)) {
    typedef FunctionInvoker<F> FunctionInvoker;
    STATIC_ASSERT(sizeof(Storage) >= sizeof(FunctionInvoker));
    STATIC_ASSERT(ALIGN_OF(Storage) >= ALIGN_OF(FunctionInvoker));
  }

  template <class F, class D>
  Callback(F func, const D& data, FunctionWithDataDummy)
    : invoker_(new (&storage_) FunctionWithDataInvoker<F, D>(func, data)) {
    typedef FunctionWithDataInvoker<F, D> FunctionWithDataInvoker;
    STATIC_ASSERT(sizeof(Storage) >= sizeof(FunctionWithDataInvoker));
    STATIC_ASSERT(ALIGN_OF(Storage) >= ALIGN_OF(FunctionWithDataInvoker));
  }

  Callback(const Callback& other)
    : invoker_(other.invoker_ ? other.invoker_->copy(&storage_) : NULL) { }

  Callback& operator=(const Callback& other) {
    invoker_ = other.invoker_ ? other.invoker_->copy(&storage_) : NULL;
    return *this;
  }

  operator bool() const { return invoker_; }

  R operator()(const Arg& arg) {
    return invoker_->invoke(arg);
  }

private:
  typedef AlignedStorage<MaxSize, ALIGN_OF(void*)> Storage;

  struct Invoker {
    virtual R invoke(const Arg& arg) = 0;
    virtual Invoker* copy(Storage* storage) = 0;
  };

  template <class F, class T>
  struct MemberInvoker : public Invoker {
    MemberInvoker(F func, T* object)
      : func(func)
      , object(object) { }

    R invoke(const Arg& arg) {
      return (object->*func)(arg);
    }

    Invoker* copy(Storage* storage) {
      return new (storage) MemberInvoker<F, T>(func, object);
    }

    F func;
    T* object;
  };

  template <class F>
  struct FunctionInvoker : public Invoker {
    FunctionInvoker(F func)
      : func(func) { }

    R invoke(const Arg& arg) {
      return func(arg);
    }

    Invoker* copy(Storage* storage) {
      return new (storage) FunctionInvoker<F>(func);
    }

    F func;
  };

  template <class F, class D>
  struct FunctionWithDataInvoker : public Invoker {
    FunctionWithDataInvoker(F func, D data)
      : func(func)
      , data(data) { }

    R invoke(const Arg& arg) {
      return func(arg, data);
    }

    Invoker* copy(Storage* storage) {
      return new (storage) FunctionWithDataInvoker<F, D>(func, data);
    }

    F func;
    D data;
  };

private:
  Invoker* invoker_;
  Storage storage_;
};

template <class R, class Arg, class T>
Callback<R, Arg> bind_callback(R (T::*func)(Arg), T* object) {
  return Callback<R, Arg>(func, object);
}

template <class R, class Arg>
Callback<R, Arg> bind_callback(R (*func)(Arg)) {
  return Callback<R, Arg>(func);
}

template <class R, class Arg, class D>
Callback<R, Arg> bind_callback(R (*func)(Arg, D), const D& data) {
  static typename Callback<R, Arg>::FunctionWithDataDummy dummy;
  return Callback<R, Arg>(func, data, dummy);
}

} // namespace cass

#endif
