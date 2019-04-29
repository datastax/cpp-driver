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

#include "token_map.hpp"

#include "token_map_impl.hpp"

using namespace datastax;
using namespace datastax::internal::core;

TokenMap::Ptr TokenMap::from_partitioner(StringRef partitioner) {
  if (ends_with(partitioner, Murmur3Partitioner::name())) {
    return Ptr(new TokenMapImpl<Murmur3Partitioner>());
  } else if (ends_with(partitioner, RandomPartitioner::name())) {
    return Ptr(new TokenMapImpl<RandomPartitioner>());
  } else if (ends_with(partitioner, ByteOrderedPartitioner::name())) {
    return Ptr(new TokenMapImpl<ByteOrderedPartitioner>());
  } else {
    LOG_WARN("Unsupported partitioner class '%s'", partitioner.to_string().c_str());
    return Ptr();
  }
}
