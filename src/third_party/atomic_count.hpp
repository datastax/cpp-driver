#ifndef __CASS_THIRD_PARTY_ATOMIC_COUNT_HPP_INCLUDED__
#define __CASS_THIRD_PARTY_ATOMIC_COUNT_HPP_INCLUDED__

// This is from boost's smart_ptr/detail
// 
// Copyright 2005-2013 Peter Dimov
// 
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#if defined(CASS_USE_STD_ATOMIC)
#include "atomic_count_std_atomic.hpp"
#elif defined(__GNUC__) && (defined(__i386__) || (__x86_64__))
#include "atomic_count_gcc_x86.hpp"
#elif defined(__GNUC__)
#include "atomic_count_gcc.hpp"
#elif defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__CYGWIN__)
#include "atomic_count_win32.hpp"
#else
#error Your compiler is not supported!
#endif

#endif
