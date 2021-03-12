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

#ifndef DATASTAX_INTERNAL_ATOMIC_HPP
#define DATASTAX_INTERNAL_ATOMIC_HPP

#include "driver_config.hpp"

#if !defined(THREAD_SANITIZER)
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define THREAD_SANITIZER 1
#endif
#elif defined(__SANITIZE_THREAD__)
#define THREAD_SANITIZER 1
#endif
#endif

// Annotations for atomic_thread_fence, see https://github.com/google/sanitizers/issues/1352
#ifdef THREAD_SANITIZER
extern "C" {
void __tsan_acquire(void* addr);
void __tsan_release(void* addr);
}
#endif

#if defined(HAVE_BOOST_ATOMIC)
#include "atomic/atomic_boost.hpp"
#elif defined(HAVE_STD_ATOMIC)
#include "atomic/atomic_std.hpp"
#else
#include "atomic/atomic_intrinsics.hpp"
#endif

#endif
