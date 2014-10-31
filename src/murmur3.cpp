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

#include <algorithm>
#include <stdlib.h>


/*
 * Algorithm derived from non-reentrant version:
 * https://github.com/datastax/ruby-driver/blob/48e4699a6e98d24702d1fc8369476720417c0384/ext/cassandra_murmur3/cassandra_murmur3.c
 * ... a modified-for-Cassandra derivative of the public domain algorithm carrying this disclaimer:
 * -----------------------------------------------------------------------------
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */


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

namespace cass {

static FORCE_INLINE int64_t rotl(int64_t x, int8_t r) {
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int64_t) (((uint64_t) x) >> (64 - r)));
}

static FORCE_INLINE int64_t fmix(int64_t k) {
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= ((uint64_t) k) >> 33;

  return k;
}

Murmur3::Murmur3()
  : h1_(0)
  , h2_(0)
  , length_(0)
  , overlap_bytes_(0) {}

static const int64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
static const int64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);
static const size_t BLOCK_SIZE = 16;

void Murmur3::update(const void* data, size_t size) {
  printf("%.*s\n", (int)size, data);
  int64_t k1;
  int64_t k2;
  const uint8_t* data_bytes = (uint8_t*)data;
  if (overlap_bytes_ > 0) {
     size_t copied_bytes = std::min(size, sizeof(overlap_buffer_) - overlap_bytes_);
     memcpy(overlap_buffer_ + overlap_bytes_, data, copied_bytes);
     overlap_bytes_ += copied_bytes;
     if (overlap_bytes_ < sizeof(overlap_buffer_)) {
       return;
     }

     k1 = *(int64_t*)overlap_buffer_;
     k2 = *(int64_t*)&overlap_buffer_[8];
     data_bytes += copied_bytes;
     size -= copied_bytes;
  } else {
    if (size >= BLOCK_SIZE) {
      k1 = *(int64_t*)data_bytes;
      k2 = *(int64_t*)(data_bytes + 8);
      data_bytes += BLOCK_SIZE;
      size -= BLOCK_SIZE;
    } else {
      memcpy(overlap_buffer_, data, size);
      overlap_bytes_ += size;
      return;
    }
  }

  const size_t contiguous_blocks = size / BLOCK_SIZE;
  const int64_t* block_data = (const int64_t*)data_bytes;

  for(size_t i = 0; i <= contiguous_blocks; ++i) {
    k1 *= c1;
    k1  = rotl(k1, 31);
    k1 *= c2;
    h1_ ^= k1;

    h1_  = rotl(h1_, 27);
    h1_ += h2_;
    h1_  = h1_ * 5 + 0x52dce729;

    k2 *= c2;
    k2  = rotl(k2, 33);
    k2 *= c1;
    h2_ ^= k2;

    h2_  = rotl(h2_, 31);
    h2_ += h1_;
    h2_  = h2_ * 5 + 0x38495ab5;

    length_ += BLOCK_SIZE;

    if (i < (contiguous_blocks - 1)) {
      k1 = block_data[i * 2];
      k2 = block_data[i * 2 + 1];
    }
  }

  size_t block_offset = contiguous_blocks * BLOCK_SIZE;
  if (block_offset < size) {
    overlap_bytes_ = size - block_offset;
    memcpy(overlap_buffer_, data_bytes + block_offset, overlap_bytes_);
  }
}

void Murmur3::final(int64_t* h1_out, int64_t* h2_out) {

  if (overlap_bytes_ > 0) {
    const uint8_t* tail = overlap_buffer_;

    int64_t k1 = 0;
    int64_t k2 = 0;

    switch(overlap_bytes_) {
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
               h2_ ^= k2;
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
               h1_ ^= k1;
    }
    length_ += overlap_bytes_;
    overlap_bytes_ = 0;
  }

  h1_ ^= length_;
  h2_ ^= length_;

  h1_ += h2_;
  h2_ += h1_;

  h1_ = fmix(h1_);
  h2_ = fmix(h2_);

  h1_ += h2_;
  h2_ += h1_;

  if (h1_out != NULL) {
    *h1_out = h1_;
  }
  if (h2_out != NULL) {
    *h2_out = h2_;
  }
  memset(this, 0, sizeof(Murmur3));
}

}
