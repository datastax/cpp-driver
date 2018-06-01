/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_MEMORY_HPP_INCLUDED__
#define __DSE_MEMORY_HPP_INCLUDED__

#include "cassandra.h"
#include "is_polymorphic.hpp"

#include <limits>
#include <new>
#include <stdlib.h>

namespace cass {

// Placement new can return a pointer to the polymorphic class instead of the
// base/allocated address in the case of multiple inheritance. In this case
// dynamic_cast<void*>() can be used to get the original base/allocated address,
// but it's only valid for polymorphic types.

template <class T, bool is_polymorphic>
struct GetAllocatedPtr {
  static void* cast(T* ptr) { return reinterpret_cast<void*>(ptr); }
};

template <class T>
struct GetAllocatedPtr<T, true> {
  static void* cast(T* ptr) { return dynamic_cast<void*>(ptr); }
};

class Memory {
public:
  static void set_functions(CassMallocFunction malloc_func,
                            CassReallocFunction realloc_func,
                            CassFreeFunction free_func);

  template <class T>
  static T* allocate() {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T();
  }

  template <class T, class Arg1>
  static T* allocate(const Arg1& arg1) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1);
  }

  template <class T, class Arg1, class Arg2>
  static T* allocate(const Arg1& arg1, const Arg2& arg2) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2);
  }

  template <class T, class Arg1, class Arg2, class Arg3>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9& arg9) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9& arg9, const Arg10& arg10) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10, class Arg11>
  static T* allocate(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9& arg9, const Arg10& arg10, const Arg11& arg11) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11);
  }

  template <class T, class Arg1>
  static T* allocate(Arg1& arg1) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1);
  }

  template <class T, class Arg1, class Arg2>
  static T* allocate(Arg1& arg1, Arg2& arg2) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2);
  }

  template <class T, class Arg1, class Arg2, class Arg3>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8, Arg9& arg9) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8, Arg9& arg9, Arg10& arg10) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
  }

  template <class T, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10, class Arg11>
  static T* allocate(Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8, Arg9& arg9, Arg10& arg10, Arg11& arg11) {
    T* ptr = reinterpret_cast<T*>(malloc(sizeof(T)));
    return new (ptr) T(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11);
  }

  template <class T>
  static void deallocate(T* ptr) {
    if (!ptr) return;
    void* base_ptr = GetAllocatedPtr<T, IsPolymorphic<T>::value>::cast(ptr);
    ptr->~T();
    free(base_ptr);
  }

  template <class T>
  static void deallocate(const T* ptr) {
    deallocate(const_cast<T*>(ptr));
  }

  static CassMallocFunction malloc_func() { return malloc_func_; }
  static CassReallocFunction realloc_func() { return realloc_func_; }
  static CassFreeFunction free_func() { return free_func_; }

  static void* malloc(size_t size) {
    if (malloc_func_ == NULL) {
      return ::malloc(size);
    }
    return malloc_func_(size);
  }

  static void* realloc(void* ptr, size_t size) {
    if (realloc_func_ == NULL) {
      return ::realloc(ptr, size);
    }
    return realloc_func_(ptr, size);
  }

  static void free(void* ptr) {
    if (free_func_ == NULL) {
      return ::free(ptr);
    }
    free_func_(ptr);
  }

private:
  static CassMallocFunction malloc_func_;
  static CassReallocFunction realloc_func_;
  static CassFreeFunction free_func_;
};

template <class T>
class DynamicArray {
public:
  DynamicArray(size_t n)
    : n_(n)
    , elements_(reinterpret_cast<T*>(Memory::malloc(sizeof(T) * n_))) {
    for (size_t i = 0; i < n_; ++i) {
      new (elements_ + i) T();
    }
  }

  DynamicArray(const DynamicArray& other)
    : n_(other.n_)
    , elements_(reinterpret_cast<T*>(Memory::malloc(sizeof(T) * n_))) {
    for (size_t i = 0; i < n_; ++i) {
      new (elements_ + i) T(other[i]);
    }
  }

  ~DynamicArray() {
    for (size_t i = 0; i < n_; ++i) {
      (elements_ + i)->~T();
    }
    Memory::free(elements_);
  }

  T& operator[](size_t index) {
    return *(elements_ + index);
  }

  const T& operator[](size_t index) const {
    return *(elements_ + index);
  }

  size_t size() const { return n_; }

  T* data() { return elements_; }

  const T* data() const { return elements_; }

private:
  const size_t n_;
  T* const elements_;
};

} // namespace cass

#endif
