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

#ifndef DATASTAX_INTERNAL_MEMORY_HPP
#define DATASTAX_INTERNAL_MEMORY_HPP

#include "cassandra.h"
#include <cstdlib>

namespace datastax { namespace internal {

class Memory {
public:
  static void set_functions(CassMallocFunction malloc_func, CassReallocFunction realloc_func,
                            CassFreeFunction free_func);

  static CassMallocFunction malloc_func() { return malloc_func_; }
  static CassReallocFunction realloc_func() { return realloc_func_; }
  static CassFreeFunction free_func() { return free_func_; }

  static void* malloc(size_t size) {
    if (malloc_func_ == NULL) {
      return std::malloc(size);
    }
    return malloc_func_(size);
  }

  static void* realloc(void* ptr, size_t size) {
    if (realloc_func_ == NULL) {
      return std::realloc(ptr, size);
    }
    return realloc_func_(ptr, size);
  }

  static void free(void* ptr) {
    if (free_func_ == NULL) {
      return std::free(ptr);
    }
    free_func_(ptr);
  }

private:
  static CassMallocFunction malloc_func_;
  static CassReallocFunction realloc_func_;
  static CassFreeFunction free_func_;
};

}} // namespace datastax::internal

#endif
