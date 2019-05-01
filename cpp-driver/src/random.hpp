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

#ifndef DATASTAX_INTERNAL_RANDOM_HPP
#define DATASTAX_INTERNAL_RANDOM_HPP

#include "allocated.hpp"
#include "third_party/mt19937_64/mt19937_64.hpp"

#include <algorithm>
#include <uv.h>

namespace datastax { namespace internal {

class Random : public Allocated {
public:
  Random();
  ~Random();

  uint64_t next(uint64_t max);

private:
  uv_mutex_t mutex_;
  MT19937_64 rng_;
};

uint64_t get_random_seed(uint64_t seed);

template <class RandomAccessIterator>
void random_shuffle(RandomAccessIterator first, RandomAccessIterator last, Random* random) {
  size_t size = last - first;
  for (size_t i = size - 1; i > 0; --i) {
    std::swap(first[i], first[random->next(i + 1)]);
  }
}

}} // namespace datastax::internal

#endif
