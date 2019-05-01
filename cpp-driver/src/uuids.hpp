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

#ifndef DATASTAX_INTERNAL_UUIDS_HPP
#define DATASTAX_INTERNAL_UUIDS_HPP

#include "allocated.hpp"
#include "atomic.hpp"
#include "cassandra.h"
#include "external.hpp"
#include "random.hpp"

#include <assert.h>
#include <string.h>
#include <uv.h>

namespace datastax { namespace internal { namespace core {

class UuidGen : public Allocated {
public:
  UuidGen();
  UuidGen(uint64_t node);
  ~UuidGen();

  void generate_time(CassUuid* output);
  void from_time(uint64_t timestamp, CassUuid* output);
  void generate_random(CassUuid* output);

private:
  void set_clock_seq_and_node(uint64_t node);
  uint64_t monotonic_timestamp();

  uint64_t clock_seq_and_node_;
  Atomic<uint64_t> last_timestamp_;

  uv_mutex_t mutex_;
  MT19937_64 ng_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::UuidGen, CassUuidGen)

#endif
