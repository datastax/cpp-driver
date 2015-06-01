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

#ifndef __CASS_HASH_INDEX_HPP_INCLUDED__
#define __CASS_HASH_INDEX_HPP_INCLUDED__

#include "fixed_vector.hpp"
#include "string_ref.hpp"

namespace cass {

class HashIndex {
public:
  typedef FixedVector<size_t, 16> IndexVec;

  struct Entry {
    Entry()
      : index(0)
      , next(NULL) { }

    StringRef name;
    size_t index;
    Entry* next;
  };

  HashIndex(size_t count);

  size_t get(StringRef name, HashIndex::IndexVec* result) const;
  void insert(Entry* entry);

private:
  FixedVector<Entry*, 32> index_;
  size_t index_mask_;
};

} // namespace cass

#endif
