/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "memory.hpp"

#include <assert.h>
#include <new>
#include <string.h>

#include <uv.h>

extern "C" {

void cass_alloc_set_functions(CassMallocFunction malloc_func,
                              CassReallocFunction realloc_func,
                              CassFreeFunction free_func) {
  cass::Memory::set_functions(malloc_func, realloc_func, free_func);
}

} // extern "C"

#ifdef DEBUG_CUSTOM_ALLOCATOR
void* operator new(size_t size) throw(std::bad_alloc) {
  assert(false && "Attempted to use global operator new");
  return cass::Memory::malloc(size);
}

void* operator new[](size_t size) throw(std::bad_alloc) {
  assert(false && "Attempted to use global operator new[]");
  return cass::Memory::malloc(size);
}

void operator delete(void* ptr) throw() {
  assert(false && "Attempted to use global operator delete");
  cass::Memory::free(ptr);
}

void operator delete[](void* ptr) throw() {
  assert(false && "Attempted to use global operator delete[]");
  cass::Memory::free(ptr);
}
#endif

namespace cass {

CassMallocFunction Memory::malloc_func_ = NULL;
CassReallocFunction Memory::realloc_func_ = NULL;
CassFreeFunction Memory::free_func_ = NULL;

#if UV_VERSION_MAJOR >= 1 && UV_VERSION_MINOR >= 6
static void* calloc_(size_t count, size_t size) {
  void* ptr = Memory::malloc(count * size);
  if (ptr != NULL) {
    memset(ptr, 0, count * size);
  }
  return ptr;
}
#endif

void Memory::set_functions(CassMallocFunction malloc_func,
                           CassReallocFunction realloc_func,
                           CassFreeFunction free_func) {
  if (malloc_func == NULL ||
      realloc_func == NULL ||
      free_func == NULL) {
    Memory::malloc_func_ = NULL;
    Memory::realloc_func_ = NULL;
    Memory::free_func_ = NULL;
  } else {
    Memory::malloc_func_ = malloc_func;
    Memory::realloc_func_ = realloc_func;
    Memory::free_func_ = free_func;
  }
#if UV_VERSION_MAJOR >= 1 && UV_VERSION_MINOR >= 6
  uv_replace_allocator(Memory::malloc_func_, Memory::realloc_func_, calloc_, Memory::free_func_);
#endif
}

} // namespace cass

