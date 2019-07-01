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

// This implementation is based of the implementation in Boost:
// "boost/atomic/detail/ops_msvc_x86.hpp". Here is the license
// and copyright from that file:

/*
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Copyright (c) 2009 Helge Bahmann
 * Copyright (c) 2012 Tim Blechmann
 * Copyright (c) 2014 Andrey Semashev
 */

#ifndef DATASTAX_INTERNAL_ATOMIC_BASE_MSVC_HPP
#define DATASTAX_INTERNAL_ATOMIC_BASE_MSVC_HPP

#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif

// Windows Server 2003 or higher required to build because
// Interlocked*64() intrinsics are used. The Boost implementation
// of Atomic<> may offer better compatibility and performance.

// This is often defined in the Visual Studio project settings
// and may need to be updated to 0x0502.
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0502
#endif

#include <Windows.h>
#include <intrin.h>

#include "macros.hpp"

#include <assert.h>

// 32-bit Windows compilation
#ifndef _M_X64
#pragma intrinsic(_InterlockedCompareExchange64)
#define InterlockedCompareExchange64 _InterlockedCompareExchange64
#endif

namespace datastax { namespace internal {

namespace impl {

// copy_cast<> wouldn't work because it would lose
// the "volatile" qualifier when passed to "memcpy()"
// and the compiler would error.
template <typename From, typename To>
inline To union_cast(const From& from) {
  STATIC_ASSERT(sizeof(From) == sizeof(To));

  union {
    To to;
    From from;
  } cast;

  cast.from = from;
  return cast.to;
}

template <class T>
struct IsIntegral {
  static const bool value = false;
};

#define IS_INTEGRAL(Type)           \
  template <>                       \
  struct IsIntegral<Type> {         \
    static const bool value = true; \
  }

IS_INTEGRAL(char);
IS_INTEGRAL(short);
IS_INTEGRAL(int);
IS_INTEGRAL(long);
IS_INTEGRAL(long long);

IS_INTEGRAL(unsigned char);
IS_INTEGRAL(unsigned short);
IS_INTEGRAL(unsigned int);
IS_INTEGRAL(unsigned long);
IS_INTEGRAL(unsigned long long);

#undef IS_INTEGRAL

template <class T, bool IsInt = IsIntegral<T>::value>
struct Classify {
  typedef void Type;
};

template <class T>
struct Classify<T, true> {
  typedef int Type;
};

template <>
struct Classify<bool, false> {
  typedef bool Type;
};

template <class T>
struct Classify<T*, false> {
  typedef void* Type;
};

template <class Type>
struct IsSigned;

#define IS_SIGNED(Type, Value)       \
  template <>                        \
  struct IsSigned<Type> {            \
    static const bool value = Value; \
  }

IS_SIGNED(int, true);
IS_SIGNED(unsigned int, false);
IS_SIGNED(long, true);
IS_SIGNED(unsigned long, false);
IS_SIGNED(long long, true);
IS_SIGNED(unsigned long long, false);

#undef IS_SIGNED

template <size_t Size, bool Signed>
struct AtomicImpl;

template <size_t NumBytes, bool Signed>
struct AtomicStorageType;

#define ATOMIC_STORAGE_TYPE(NumBytes, Signed, StorageType) \
  template <>                                              \
  struct AtomicStorageType<NumBytes, Signed> {             \
    typedef StorageType Type;                              \
  }

ATOMIC_STORAGE_TYPE(4, true, long);
ATOMIC_STORAGE_TYPE(4, false, unsigned long);
ATOMIC_STORAGE_TYPE(8, true, __int64);
ATOMIC_STORAGE_TYPE(8, false, unsigned __int64);

#undef ATOMIC_STORAGE_TYPE

template <bool Signed>
struct AtomicImpl<4, Signed> {
  typedef typename AtomicStorageType<4, Signed>::Type Type;

  static inline Type fetch_add(volatile Type& storage, Type value) {
    return static_cast<Type>(InterlockedExchangeAdd((long*)&storage, (long)value));
  }

  static inline Type fetch_sub(volatile Type& storage, Type value) {
    typedef typename AtomicStorageType<4, true>::Type SignedType;
    return fetch_add(storage, static_cast<Type>(-static_cast<SignedType>(value)));
  }

  static inline Type exchange(volatile Type& storage, Type value) {
    return static_cast<Type>(InterlockedExchange((long*)&storage, (long)value));
  }

  static inline bool compare_exchange(volatile Type& storage, Type& expected, Type desired) {
    Type temp_expected = expected;
    Type previous = static_cast<Type>(
        InterlockedCompareExchange((long*)&storage, (long)desired, (long)temp_expected));
    expected = previous;
    return (previous == temp_expected);
  }
};

template <bool Signed>
struct AtomicImpl<8, Signed> {
  typedef typename AtomicStorageType<8, Signed>::Type Type;

  static inline Type fetch_add(volatile Type& storage, Type value) {
#ifdef _M_X64
    return static_cast<Type>(InterlockedExchangeAdd64((__int64*)&storage, (__int64)value));
#else
    Type old_value = storage;
    while (!compare_exchange(storage, old_value, (old_value + value))) {
    }
    return old_value;
#endif
  }

  static inline Type fetch_sub(volatile Type& storage, Type value) {
    typedef typename AtomicStorageType<4, true>::Type SignedType;
    return fetch_add(storage, static_cast<Type>(-static_cast<SignedType>(value)));
  }

  static inline Type exchange(volatile Type& storage, Type value) {
#ifdef _M_X64
    return static_cast<Type>(InterlockedExchange64((__int64*)&storage, (__int64)value));
#else
    Type ret = storage;
    while (!compare_exchange(storage, ret, value)) {
    }
    return ret;
#endif
  }

  static inline bool compare_exchange(volatile Type& storage, Type& expected, Type desired) {
    Type temp_expected = expected;
    Type previous = static_cast<Type>(
        InterlockedCompareExchange64((__int64*)&storage, (__int64)desired, (__int64)temp_expected));
    expected = previous;
    return (previous == temp_expected);
  }
};

template <class T, class Kind>
class AtomicBase;

// User defined types (e.g. enums)
template <class T>
class AtomicBase<T, void> {
public:
  typedef typename impl::AtomicImpl<sizeof(T), false> Impl;
  typedef typename Impl::Type ImplType;

  AtomicBase() {}
  explicit AtomicBase(T value)
      : value_(static_cast<ImplType>(value)) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    assert(order != MEMORY_ORDER_ACQUIRE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    if (order != MEMORY_ORDER_SEQ_CST) {
      _ReadWriteBarrier();
      value_ = static_cast<ImplType>(value);
      _ReadWriteBarrier();
    } else {
      Impl::exchange(value_, static_cast<ImplType>(value));
    }
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    assert(order != MEMORY_ORDER_RELEASE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    _ReadWriteBarrier();
    T result = static_cast<T>(value_);
    _ReadWriteBarrier();
    return result;
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return static_cast<T>(Impl::exchange(value_, static_cast<ImplType>(value)));
  }

  inline bool compare_exchange_strong(T& expected, T desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    ImplType temp_expected = static_cast<ImplType>(expected);
    bool result = Impl::compare_exchange(value_, temp_expected, static_cast<ImplType>(desired));
    expected = static_cast<T>(temp_expected);
    return result;
  }

  inline bool compare_exchange_weak(T& expected, T desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return compare_exchange_strong(expected, desired, order);
  }

private:
  typename ImplType value_;
};

// Boolean
template <class T>
class AtomicBase<T, bool> {
public:
  typedef typename impl::AtomicImpl<4, false> Impl;
  typedef typename Impl::Type ImplType;

  AtomicBase() {}
  explicit AtomicBase(T value)
      : value_(cast(value)) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    assert(order != MEMORY_ORDER_ACQUIRE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    if (order != MEMORY_ORDER_SEQ_CST) {
      _ReadWriteBarrier();
      value_ = cast(value);
      _ReadWriteBarrier();
    } else {
      Impl::exchange(value_, cast(value));
    }
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    assert(order != MEMORY_ORDER_RELEASE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    _ReadWriteBarrier();
    T result = cast(value_);
    _ReadWriteBarrier();
    return result;
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return cast(Impl::exchange(value_, cast(value)));
  }

  inline bool compare_exchange_strong(T& expected, T desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    ImplType temp_expected = cast(expected);
    bool result = Impl::compare_exchange(value_, temp_expected, cast(desired));
    expected = cast(temp_expected);
    return result;
  }

  inline bool compare_exchange_weak(T& expected, T desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return compare_exchange_strong(expected, desired, order);
  }

private:
  static inline bool cast(ImplType value) { return static_cast<ImplType>(value ? 1 : 0); }

  static inline ImplType cast(bool value) { return static_cast<T>(value != 0); }

  typename ImplType value_;
};

// Integer types
template <class T>
class AtomicBase<T, int> {
public:
  typedef typename impl::AtomicImpl<sizeof(T), impl::IsSigned<T>::value> Impl;
  typedef typename Impl::Type ImplType;

  AtomicBase() {}
  explicit AtomicBase(T value)
      : value_(static_cast<ImplType>(value)) {}

  inline void store(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    assert(order != MEMORY_ORDER_ACQUIRE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    if (order != MEMORY_ORDER_SEQ_CST) {
      _ReadWriteBarrier();
      value_ = static_cast<ImplType>(value);
      _ReadWriteBarrier();
    } else {
      Impl::exchange(value_, static_cast<ImplType>(value));
    }
  }

  inline T load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    assert(order != MEMORY_ORDER_RELEASE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    _ReadWriteBarrier();
    T result = static_cast<T>(value_);
    _ReadWriteBarrier();
    return result;
  }

  inline T fetch_add(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return static_cast<T>(Impl::fetch_add(value_, static_cast<ImplType>(value)));
  }

  inline T fetch_sub(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return static_cast<T>(Impl::fetch_sub(value_, static_cast<ImplType>(value)));
  }

  inline T exchange(T value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return static_cast<T>(Impl::exchange(value_, static_cast<ImplType>(value)));
  }

  inline bool compare_exchange_strong(T& expected, T desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    ImplType temp_expected = static_cast<ImplType>(expected);
    bool result = Impl::compare_exchange(value_, temp_expected, static_cast<ImplType>(desired));
    expected = static_cast<T>(temp_expected);
    return result;
  }

  inline bool compare_exchange_weak(T& expected, T desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return compare_exchange_strong(expected, desired, order);
  }

private:
  typename ImplType value_;
};

// Pointer types
template <class T>
class AtomicBase<T*, void*> {
public:
  typedef typename impl::AtomicImpl<sizeof(T*), false> Impl;
  typedef typename Impl::Type ImplType;

  AtomicBase() {}
  explicit AtomicBase(T* value)
      : value_(union_cast<T*, ImplType>(value)) {}

  inline void store(T* value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    assert(order != MEMORY_ORDER_ACQUIRE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    if (order != MEMORY_ORDER_SEQ_CST) {
      _ReadWriteBarrier();
      value_ = union_cast<T*, ImplType>(value);
      _ReadWriteBarrier();
    } else {
      Impl::exchange(value_, union_cast<T*, ImplType>(value));
    }
  }

  inline T* load(MemoryOrder order = MEMORY_ORDER_SEQ_CST) const volatile {
    assert(order != MEMORY_ORDER_RELEASE);
    assert(order != MEMORY_ORDER_CONSUME);
    assert(order != MEMORY_ORDER_ACQ_REL);
    _ReadWriteBarrier();
    T* result = union_cast<volatile ImplType, T*>(value_);
    _ReadWriteBarrier();
    return result;
  }

  inline T* exchange(T* value, MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return union_cast<volatile ImplType, T*>(
        Impl::exchange(value_, union_cast<T*, ImplType>(value)));
  }

  inline bool compare_exchange_strong(T*& expected, T* desired,
                                      MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    ImplType temp_expected = union_cast<T*, ImplType>(expected);
    bool result = Impl::compare_exchange(value_, temp_expected, union_cast<T*, ImplType>(desired));
    expected = union_cast<volatile ImplType, T*>(temp_expected);
    return result;
  }

  inline bool compare_exchange_weak(T*& expected, T* desired,
                                    MemoryOrder order = MEMORY_ORDER_SEQ_CST) volatile {
    return compare_exchange_strong(expected, desired, order);
  }

private:
  typename ImplType value_;
};

} // namespace impl

template <class T>
class Atomic : public impl::AtomicBase<T, typename impl::Classify<T>::Type> {
public:
  Atomic() {}
  explicit Atomic(T value)
      : AtomicBase(value) {}
};

inline void atomic_thread_fence(MemoryOrder order) {
  _ReadWriteBarrier();
  if (order == MEMORY_ORDER_SEQ_CST) {
    long temp;
    InterlockedExchange(&temp, 0);
  }
  _ReadWriteBarrier();
}

}} // namespace datastax::internal

#endif
