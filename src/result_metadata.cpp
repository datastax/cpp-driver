/*
  Copyright (c) 2014-2015 DataStax

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

namespace cass {

ResultMetadata::ResultMetadata(size_t column_count)
  : index_(column_count) {
  defs_.reserve(column_count);
}

size_t ResultMetadata::get_indices(StringRef name,
                           HashIndex::IndexVec* result) const{
  return index_.get(name, result);
}

void ResultMetadata::insert(ColumnDefinition& def) {
  defs_.push_back(def);
  index_.insert(&defs_.back());
}

} // namespace cass
