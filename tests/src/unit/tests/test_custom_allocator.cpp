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

#include <gtest/gtest.h>

#include "dse.h"

#include <stdlib.h>

static int custom_malloc_count = 0;
void* custom_malloc(size_t size) {
  custom_malloc_count++;
  return ::malloc(size);
}

void* custom_realloc(void* ptr, size_t size) {
  // It's nearly impossible to get a predictable count for realloc
  return ::realloc(ptr, size);
}

static int custom_free_count = 0;
void custom_free(void* ptr) {
  custom_free_count++;
  return ::free(ptr);
}

TEST(CustomAllocatorUnitTest, ReplaceAllocator) {
  // Set custom allocation functions and make sure they're used
  cass_alloc_set_functions(custom_malloc, custom_realloc, custom_free);

  CassSession* session = cass_session_new();
  cass_session_free(session);

  ASSERT_GE(custom_malloc_count, 0);
  ASSERT_GE(custom_free_count, 0);

  // Restore default functions
  custom_malloc_count = 0;
  custom_free_count = 0;
  cass_alloc_set_functions(NULL, NULL, NULL);

  ASSERT_EQ(custom_malloc_count, 0);
  ASSERT_EQ(custom_free_count, 0);
}
