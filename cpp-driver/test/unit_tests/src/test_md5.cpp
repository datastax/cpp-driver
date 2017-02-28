/*
  Copyright (c) 2014-2016 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "md5.hpp"

#include <ctype.h>
#include <stdio.h>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(md5)

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
  cass::Md5 m;
  m.update(reinterpret_cast<const uint8_t*>(data), strlen(data));
  uint8_t hash[16];
  m.final(hash);
  return hash_equal(hash, hash_str);
}

BOOST_AUTO_TEST_CASE(simple)
{
  BOOST_CHECK(check_hash("", "d41d8cd98f00b204e9800998ecf8427e"));
  BOOST_CHECK(check_hash("a", "0cc175b9c0f1b6a831c399e269772661"));
  BOOST_CHECK(check_hash("abc", "900150983cd24fb0d6963f7d28e17f72"));

  const char* big_str =
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
      "012345689abcdef012345689abcdef012345689abcdef012345689abcdef"
      "012345689abcdef012345689abcdef012345689abcdef012345689abcdef";

  BOOST_CHECK(check_hash(big_str, "15355dec7c48faeb01b46366d90be0be"));
}

BOOST_AUTO_TEST_SUITE_END()
