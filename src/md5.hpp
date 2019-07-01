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

// Based on public domain source found here:
// http://openwall.info/wiki/people/solar/software/public-domain-source-code/md5

#ifndef DATASTAX_INTERNAL_MD5_HPP
#define DATASTAX_INTERNAL_MD5_HPP

#include "macros.hpp"

#include <stdint.h>
#include <uv.h>

namespace datastax { namespace internal {

class Md5 {
public:
  Md5();

  void update(const uint8_t* data, size_t size);
  void final(uint8_t* result);

private:
  const uint8_t* body(const uint8_t* data, size_t size);

private:
  // Any 32-bit or wider unsigned integer data type will do
  typedef uint32_t MD5_u32plus;

  MD5_u32plus lo_, hi_;
  MD5_u32plus a_, b_, c_, d_;
  unsigned char buffer_[64];
  MD5_u32plus block_[16];

  DISALLOW_COPY_AND_ASSIGN(Md5);
};

}} // namespace datastax::internal

#endif
