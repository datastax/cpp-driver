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

#include "md5.hpp"

using datastax::internal::Md5;

static bool hash_equal(uint8_t* hash, const char* hash_str) {
  const char* p = hash_str;
  for (size_t i = 0; i < 16; ++i) {
    unsigned int v;
    sscanf(p, "%2x", &v);
    if (hash[i] != v) {
      return false;
    }
    p += 2;
  }
  return true;
}

static bool check_hash(const char* data, const char* hash_str) {
  Md5 m;
  m.update(reinterpret_cast<const uint8_t*>(data), strlen(data));
  uint8_t hash[16];
  m.final(hash);
  return hash_equal(hash, hash_str);
}

TEST(Md5UnitTest, Simple) {
  EXPECT_TRUE(check_hash("", "d41d8cd98f00b204e9800998ecf8427e"));
  EXPECT_TRUE(check_hash("a", "0cc175b9c0f1b6a831c399e269772661"));
  EXPECT_TRUE(check_hash("abc", "900150983cd24fb0d6963f7d28e17f72"));

  const char* big_str = "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
                        "012345689abcdef012345689abcdef012345689abcdef012345689abcdef";

  EXPECT_TRUE(check_hash(big_str, "15355dec7c48faeb01b46366d90be0be"));
}
