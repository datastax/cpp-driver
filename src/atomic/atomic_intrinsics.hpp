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

// These implmentations are based on the intrinics implmentations
// of boost::atomic<> for GCC and MSVC. Here is the license and
// copyright from those files:

/*
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Copyright (c) 2009 Helge Bahmann
 * Copyright (c) 2012 Tim Blechmann
 * Copyright (c) 2014 Andrey Semashev
 */

#ifndef DATASTAX_INTERNAL_ATOMIC_INTRINSICS_HPP
#define DATASTAX_INTERNAL_ATOMIC_INTRINSICS_HPP

// The slower, intrinsics-based implemenations of Atomic<>.

// Note: Please use the boost::atomic<> or std::atomic<>
// implementations when possible.

namespace datastax { namespace internal {

enum MemoryOrder {
  MEMORY_ORDER_RELAXED = 0,
  MEMORY_ORDER_CONSUME = 1,
  MEMORY_ORDER_ACQUIRE = 2,
  MEMORY_ORDER_RELEASE = 4,
  MEMORY_ORDER_ACQ_REL = 6, // MEMORY_ORDER_ACQUIRE | MEMORY_ORDER_RELEASE
  MEMORY_ORDER_SEQ_CST = 14 // MEMORY_ORDER_ACQ_REL | 8
};

}} // namespace datastax::internal

#if defined(__GNUC__)
#include "atomic_intrinsics_gcc.hpp"
#elif defined(_MSC_VER)
#include "atomic_intrinsics_msvc.hpp"
#else
#error Unsupported compiler!
#endif

#endif
