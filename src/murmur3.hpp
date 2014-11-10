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

#ifndef __CASS_MURMUR3_HPP_INCLUDED__
#define __CASS_MURMUR3_HPP_INCLUDED__

#include "macros.hpp"

#include <boost/cstdint.hpp>

namespace cass {

class Murmur3 {
public:
  Murmur3();

  void update(const void* data, size_t size);
  void final(int64_t* h1_out, int64_t* h2_out);

private:
  int64_t h1_;
  int64_t h2_;
  size_t length_;
  uint8_t overlap_buffer_[16];
  size_t overlap_bytes_;

  DISALLOW_COPY_AND_ASSIGN(Murmur3);
};

} // namespace cass

#endif
