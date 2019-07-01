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

#include "token_map_impl.hpp"

#include "md5.hpp"
#include "murmur3.hpp"

using namespace datastax;
using namespace datastax::internal::core;

static int64_t parse_int64(const char* p, size_t n) {
  int c;
  const char* s = p;
  for (; n != 0 && isspace(c = *s); ++s, --n) {
  }

  if (n == 0) {
    return 0;
  }

  int64_t sign = 1;
  if (c == '-') {
    sign = -1;
    ++s;
    --n;
  }

  int64_t value = 0;
  for (; n != 0 && isdigit(c = *s); ++s, --n) {
    value *= 10;
    value += c - '0';
  }

  return sign * value;
}

static void parse_int128(const char* p, size_t n, uint64_t* h, uint64_t* l) {
  // no sign handling because C* uses [0, 2^127]
  int c;
  const char* s = p;

  for (; n != 0 && isspace(c = *s); ++s, --n) {
  }

  if (n == 0) {
    *h = *l = 0;
    return;
  }

  uint64_t hi = 0;
  uint64_t lo = 0;
  uint64_t hi_tmp;
  uint64_t lo_tmp;
  uint64_t lo_tmp2;
  for (; n != 0 && isdigit(c = *s); ++s, --n) {
    hi_tmp = hi;
    lo_tmp = lo;

    // value *= 10;
    lo = lo_tmp << 1;
    hi = (lo_tmp >> 63) + (hi_tmp << 1);
    lo_tmp2 = lo;
    lo += lo_tmp << 3;
    hi += (lo_tmp >> 61) + (hi_tmp << 3) + (lo < lo_tmp2 ? 1 : 0);

    // value += c - '0';
    lo_tmp = lo;
    lo += c - '0';
    hi += (lo < lo_tmp) ? 1 : 0;
  }

  *h = hi;
  *l = lo;
}

const uint32_t IdGenerator::EMPTY_KEY(0);
const uint32_t IdGenerator::DELETED_KEY(CASS_UINT32_MAX);

Murmur3Partitioner::Token Murmur3Partitioner::from_string(const StringRef& str) {
  return parse_int64(str.data(), str.size());
}

Murmur3Partitioner::Token Murmur3Partitioner::hash(const StringRef& str) {
  return MurmurHash3_x64_128(str.data(), str.size(), 0);
}

RandomPartitioner::Token RandomPartitioner::from_string(const StringRef& str) {
  Token token;
  parse_int128(str.data(), str.size(), &token.hi, &token.lo);
  return token;
}

uint64_t RandomPartitioner::encode(uint8_t* bytes) {
  uint64_t result = 0;
  const size_t num_bytes = sizeof(uint64_t);
  for (size_t i = 0; i < num_bytes; ++i) {
    result |= (static_cast<uint64_t>(bytes[i]) << (8 * (num_bytes - i - 1)));
  }
  return result;
}

RandomPartitioner::Token RandomPartitioner::abs(RandomPartitioner::Token token) {
  if (token.hi & 0x8000000000000000ULL) {
    token.hi = ~token.hi;
    token.lo = ~token.lo;

    uint64_t old_lo = token.lo;
    ++token.lo;
    // Carry to "hi" if our "lo" value wrapped
    if (token.lo < old_lo) {
      ++token.hi;
    }
  }
  return token;
}

RandomPartitioner::Token RandomPartitioner::hash(const StringRef& str) {
  Md5 hash;
  hash.update(reinterpret_cast<const uint8_t*>(str.data()), str.size());
  uint8_t digest[16];
  hash.final(digest);
  Token token;

  // For compatability with Cassandra we interpret the MD5 as a big-endian value:
  // Reference:
  // https://docs.oracle.com/javase/7/docs/api/java/math/BigInteger.html#BigInteger(byte[])
  token.hi = encode(digest);
  token.lo = encode(digest + 8);

  // Then we find the absolute value of the two's complement representation.
  token = abs(token);

  return token;
}

ByteOrderedPartitioner::Token ByteOrderedPartitioner::from_string(const StringRef& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  return Token(data, data + str.size());
}

ByteOrderedPartitioner::Token ByteOrderedPartitioner::hash(const StringRef& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  return Token(data, data + str.size());
}
