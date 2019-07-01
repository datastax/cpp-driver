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

/*-----------------------------------------------------------------------------
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

#include "murmur3.hpp"

#include <algorithm>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER)

#define FORCE_INLINE __forceinline

#define ROTL32(x, y) _rotl(x, y)
#define ROTL64(x, y) _rotl64(x, y)

#define BIG_CONSTANT(x) (x)

// Other compilers

#else // defined(_MSC_VER)

#define FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32(int32_t x, int8_t r) {
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int32_t)(((uint32_t)x) >> (32 - r)));
}

inline int64_t rotl64(int64_t x, int8_t r) {
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int64_t)(((uint64_t)x) >> (64 - r)));
}

#define ROTL32(x, y) rotl32(x, y)
#define ROTL64(x, y) rotl64(x, y)

#define BIG_CONSTANT(x) (x##LL)

#endif // !defined(_MSC_VER)

namespace datastax { namespace internal {

FORCE_INLINE int64_t getblock(const int64_t* p, int i) { return p[i]; }

// Finalization mix - force all bits of a hash block to avalanche
FORCE_INLINE int64_t fmix(int64_t k) {
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  k ^= ((uint64_t)k) >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= ((uint64_t)k) >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= ((uint64_t)k) >> 33;

  return k;
}

int64_t MurmurHash3_x64_128(const void* key, const int len, const uint32_t seed) {
  const int8_t* data = (const int8_t*)key;
  const int nblocks = len / 16;

  int64_t h1 = seed;
  int64_t h2 = seed;

  int64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  int64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);
  int64_t k1 = 0;
  int64_t k2 = 0;

  const int64_t* blocks = (const int64_t*)(data);
  const int8_t* tail = (const int8_t*)(data + nblocks * 16);

  //----------
  // body

  int i;
  for (i = 0; i < nblocks; i++) {
    int64_t k1 = getblock(blocks, i * 2 + 0);
    int64_t k2 = getblock(blocks, i * 2 + 1);

    k1 *= c1;
    k1 = ROTL64(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = ROTL64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2 = ROTL64(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = ROTL64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  //----------
  // tail
  switch (len & 15) {
    case 15:
      k2 ^= ((int64_t)(tail[14])) << 48; /* FALLTHRU */
    case 14:
      k2 ^= ((int64_t)(tail[13])) << 40; /* FALLTHRU */
    case 13:
      k2 ^= ((int64_t)(tail[12])) << 32; /* FALLTHRU */
    case 12:
      k2 ^= ((int64_t)(tail[11])) << 24; /* FALLTHRU */
    case 11:
      k2 ^= ((int64_t)(tail[10])) << 16; /* FALLTHRU */
    case 10:
      k2 ^= ((int64_t)(tail[9])) << 8; /* FALLTHRU */
    case 9:
      k2 ^= ((int64_t)(tail[8])) << 0;
      k2 *= c2;
      k2 = ROTL64(k2, 33);
      k2 *= c1;
      h2 ^= k2; /* FALLTHRU */

    case 8:
      k1 ^= ((int64_t)(tail[7])) << 56; /* FALLTHRU */
    case 7:
      k1 ^= ((int64_t)(tail[6])) << 48; /* FALLTHRU */
    case 6:
      k1 ^= ((int64_t)(tail[5])) << 40; /* FALLTHRU */
    case 5:
      k1 ^= ((int64_t)(tail[4])) << 32; /* FALLTHRU */
    case 4:
      k1 ^= ((int64_t)(tail[3])) << 24; /* FALLTHRU */
    case 3:
      k1 ^= ((int64_t)(tail[2])) << 16; /* FALLTHRU */
    case 2:
      k1 ^= ((int64_t)(tail[1])) << 8; /* FALLTHRU */
    case 1:
      k1 ^= ((int64_t)(tail[0])) << 0;
      k1 *= c1;
      k1 = ROTL64(k1, 31);
      k1 *= c2;
      h1 ^= k1; /* FALLTHRU */
  };

  //----------
  // finalization

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix(h1);
  h2 = fmix(h2);

  h1 += h2;
  h2 += h1;

  return h1;
}

}} // namespace datastax::internal
