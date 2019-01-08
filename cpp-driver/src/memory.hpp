/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_MEMORY_HPP_INCLUDED__
#define __DSE_MEMORY_HPP_INCLUDED__

#include "cassandra.h"
#include <cstdlib>

namespace cass {

class Memory {
public:
  static void set_functions(CassMallocFunction malloc_func,
                            CassReallocFunction realloc_func,
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

} // namespace cass

#endif
