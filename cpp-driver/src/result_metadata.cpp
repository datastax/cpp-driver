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

// The FNV1a hash code is based on work found here:
// http://www.isthe.com/chongo/tech/comp/fnv/index.html
// and is therefore public domain.

#include "result_metadata.hpp"

#include "utils.hpp"

#include <iterator>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

ResultMetadata::ResultMetadata(size_t column_count, const RefBuffer::Ptr& buffer)
    : defs_(column_count)
    , buffer_(buffer) {}

size_t ResultMetadata::get_indices(StringRef name, IndexVec* result) const {
  return defs_.get_indices(name, result);
}

void ResultMetadata::add(const ColumnDefinition& def) { defs_.add(def); }
