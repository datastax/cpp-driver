/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

// The implementation of cass::IsPolymorphic<> was taken from Boost and is under
// this copyright/license:

//  (C) Copyright John Maddock 2000.
//  Use, modification and distribution are subject to the Boost Software License,
//  Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt).
//
//  See http://www.boost.org/libs/type_traits for most recent version including documentation.

#ifndef __CASS_IS_POLYMORPHIC_HPP_INCLUDED__
#define __CASS_IS_POLYMORPHIC_HPP_INCLUDED__

#ifdef _MSC_VER
#include <type_traits>
#endif

namespace cass {

#ifdef _MSC_VER
template<class T>
class IsPolymorphic : public std::is_polymorphic<T> { };
#else
struct FalseType { static const bool value = false; };
struct TrueType { static const bool value = true; };

template <class T> struct IsIntegral : public FalseType { };
template <class T> struct IsIntegral<const T> : public IsIntegral<T> { };
template<> struct IsIntegral<signed char> : public TrueType { };
template<> struct IsIntegral<short> : public TrueType { };
template<> struct IsIntegral<int> : public TrueType { };
template<> struct IsIntegral<long> : public TrueType { };
template<> struct IsIntegral<long long> : public TrueType { };
template<> struct IsIntegral<unsigned char> : public TrueType { };
template<> struct IsIntegral<unsigned short> : public TrueType { };
template<> struct IsIntegral<unsigned int> : public TrueType { };
template<> struct IsIntegral<unsigned long> : public TrueType { };
template<> struct IsIntegral<unsigned long long> : public TrueType { };
template<> struct IsIntegral<bool> : public TrueType { };

template <class T> struct IsFloating : public FalseType { };
template <class T> struct IsFloating<const T> : public IsFloating<T> { };
template <> struct IsFloating<float> : public FalseType { };
template <> struct IsFloating<double> : public FalseType { };

template <class T> struct IsReference : public FalseType { };
template <class T> struct IsReference<T&> : public TrueType { };

template <class T> struct IsArray : public FalseType { };
template <class T> struct IsArray<T[]> : public TrueType { };
template <class T> struct IsArray<T const[]> : public TrueType { };

template <class T> struct IsVoid : public FalseType { };
template <> struct IsVoid<void> : public TrueType { };
template <> struct IsVoid<const void> : public TrueType { };

// *Important* This doesn't work for enums and unions
template<class T>
struct IsClass {
  static const bool value= !IsIntegral<T>::value && !IsFloating<T>::value &&
                           !IsReference<T>::value && !IsArray<T>::value &&
                           !IsVoid<T>::value;
};

// This is how Boost implements its version of is_polymorphic<> which seems to
// be compatabile with the compilers we care about. This checks the difference between
// a virtual derived class and a non-virtual derived class and if the size are the same
// then it's a polymorphic type because adding a virtual destructor in a derived class
// didn't add vtable pointer to the size.
template <class T>
struct IsPolymorphicImpl {
  struct Type1 : public T {
    Type1();
    ~Type1();
    Type1(const Type1&);
    Type1& operator=(const Type1&);
    char padding[256];
  };

  struct Type2 : public T {
    Type2();
    virtual ~Type2(); // Add a vtable to this type
    Type2(const Type2&);
    Type2& operator=(const Type2&);
    char padding[256];
  };

  static const bool value = (sizeof(Type1) == sizeof(Type2));
};

template <class T>
class IsPolymorphicImplNotClass : public FalseType { };

template <bool is_class>
struct IsPolymorphicSelector {
  template <class T>
  struct Rebind {
    typedef IsPolymorphicImplNotClass<T> Type;
  };
};

template <>
struct IsPolymorphicSelector<true> {
  template <class T>
  struct Rebind {
    typedef IsPolymorphicImpl<T> Type;
  };
};

template <class T>
struct IsPolymorphic {
  typedef IsPolymorphicSelector<IsClass<T>::value> Selector;
  typedef typename Selector::template Rebind<T> Binder;
  static const bool value = Binder::Type::value;
};

#endif

} // namespace cass

#endif
