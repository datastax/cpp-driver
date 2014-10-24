/*
  Copyright 2014 DataStax

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

#include "murmur3.hpp"

/*-----------------------------------------------------------------------------
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

#if 0
#ifdef _MSC_VER
typedef unsigned __int64 uint64_t;
typedef __int64 int64_t;

#define FORCE_INLINE __forceinline
#else
#include <stdint.h>
#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline)) inline
#else
#define FORCE_INLINE inline
#endif
#endif

#define BIG_CONSTANT(x) (x##LLU)

static FORCE_INLINE int64_t
rotl(int64_t x, int8_t r)
{
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int64_t) (((uint64_t) x) >> (64 - r)));
}

static FORCE_INLINE int64_t
getblock(const int64_t* p, int i)
{
  return p[i];
}

static FORCE_INLINE int64_t
fmix(int64_t k)
{
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= ((uint64_t) k) >> 33;

  return k;
}

static VALUE
rb_murmur3_hash(VALUE self, VALUE rb_string)
{
  char*   bytes;
  int64_t length;

  Check_Type(rb_string, T_STRING);

  bytes  = RSTRING_PTR(rb_string);
  length = RSTRING_LEN(rb_string);

  const int nblocks = length / 16;
  int i;

  int64_t h1 = 0;
  int64_t h2 = 0;

  int64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  int64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body

  const int64_t* blocks = (const int64_t*)(bytes);

  for(i = 0; i < nblocks; i++)
  {
    int64_t k1 = getblock(blocks, i * 2 + 0);
    int64_t k2 = getblock(blocks, i * 2 + 1);

    k1 *= c1;
    k1  = rotl(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1  = rotl(h1, 27);
    h1 += h2;
    h1  = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2  = rotl(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2  = rotl(h2, 31);
    h2 += h1;
    h2  = h2 * 5 + 0x38495ab5;
  }

  //----------
  // tail

  const int8_t* tail = (const int8_t*) (bytes + nblocks * 16);

  int64_t k1 = 0;
  int64_t k2 = 0;

  switch(length & 15)
  {
    case 15: k2 ^= ((int64_t) tail[14]) << 48;
    case 14: k2 ^= ((int64_t) tail[13]) << 40;
    case 13: k2 ^= ((int64_t) tail[12]) << 32;
    case 12: k2 ^= ((int64_t) tail[11]) << 24;
    case 11: k2 ^= ((int64_t) tail[10]) << 16;
    case 10: k2 ^= ((int64_t) tail[ 9]) << 8;
    case  9: k2 ^= ((int64_t) tail[ 8]) << 0;
             k2 *= c2;
             k2  = rotl(k2, 33);
             k2 *= c1;
             h2 ^= k2;
    case  8: k1 ^= ((int64_t) tail[7]) << 56;
    case  7: k1 ^= ((int64_t) tail[6]) << 48;
    case  6: k1 ^= ((int64_t) tail[5]) << 40;
    case  5: k1 ^= ((int64_t) tail[4]) << 32;
    case  4: k1 ^= ((int64_t) tail[3]) << 24;
    case  3: k1 ^= ((int64_t) tail[2]) << 16;
    case  2: k1 ^= ((int64_t) tail[1]) << 8;
    case  1: k1 ^= ((int64_t) tail[0]) << 0;
             k1 *= c1;
             k1  = rotl(k1, 31);
             k1 *= c2;
             h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= length;
  h2 ^= length;

  h1 += h2;
  h2 += h1;

  h1 = fmix(h1);
  h2 = fmix(h2);

  h1 += h2;
  h2 += h1;

  return LL2NUM(h1);
}

void
Init_cassandra_murmur3()
{
  VALUE mCassandra, mMurmur3;

  mCassandra = rb_define_module_under(rb_cObject, "Cassandra");
  mMurmur3   = rb_define_module_under(mCassandra, "Murmur3");

  rb_define_module_function(mMurmur3, "hash", rb_murmur3_hash, 1);
}
#endif
