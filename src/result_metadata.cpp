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

#include "result_metadata.hpp"

#include "common.hpp"

#include "third_party/boost/boost/functional/hash.hpp"
#include "third_party/boost/boost/algorithm/string.hpp"

#include <iterator>

// This can be decreased to reduce hash collisions, but it will require
// additional memory.
#define LOAD_FACTOR 0.75

namespace {

struct ToLowerIterator {
public:
  typedef boost::string_ref::value_type value_type;
  typedef boost::string_ref::difference_type difference_type;
  typedef boost::string_ref::pointer pointer;
  typedef boost::string_ref::reference reference;
  typedef std::forward_iterator_tag iterator_category;

  ToLowerIterator(boost::string_ref::iterator it)
    : it_(it) {}

  value_type operator*() { return ::tolower(*it_); }

  ToLowerIterator operator++() { ++it_; return *this; }

  bool operator!=(const ToLowerIterator& rhs) { return it_ != rhs.it_; }

private:
    boost::string_ref::const_iterator it_;
};

} // namespace

namespace cass {

ResultMetadata::ResultMetadata(size_t column_count) {
  defs_.reserve(column_count);

  size_t index_size = next_pow_2(static_cast<size_t>(column_count / LOAD_FACTOR) + 1);
  index_.resize(index_size, NULL);
  index_mask_ = index_size - 1;
}


size_t ResultMetadata::get(boost::string_ref name,
                     ResultMetadata::IndexVec* result) const{
  result->clear();
  bool is_case_sensitive = false;

  if (name.size() > 0 && name.front() == '"' && name.back() == '"') {
    is_case_sensitive = true;
    name = name.substr(1, name.size() - 2);
  }

  size_t h = boost::hash_range(ToLowerIterator(name.begin()),
                               ToLowerIterator(name.end())) & index_mask_;

  size_t start = h;
  while (index_[h] != NULL && !boost::iequals(name,
                                              boost::string_ref(index_[h]->name,
                                                                index_[h]->name_size))) {
    h = (h + 1) & index_mask_;
    if (h == start) {
      return 0;
    }
  }

  const ColumnDefinition* def = index_[h];

  if (def == NULL) {
    return 0;
  }

  if (!is_case_sensitive) {
    while(def != NULL) {
      result->push_back(def->index);
      def = def->next;
    }
  } else {
    while(def != NULL) {
      if (name.compare(boost::string_ref(def->name, def->name_size)) == 0) {
        result->push_back(def->index);
      }
      def = def->next;
    }
  }

  return result->size();
}

void ResultMetadata::insert(ColumnDefinition& def) {
  defs_.push_back(def);

  boost::string_ref name(def.name, def.name_size);

  size_t h = boost::hash_range(ToLowerIterator(name.begin()),
                               ToLowerIterator(name.end())) & index_mask_;

  if (index_[h] == NULL) {
    index_[h] = &defs_.back();
  } else {
    // Use linear probing to find an open bucket
    while (index_[h] != NULL && !boost::iequals(name,
                                                boost::string_ref(index_[h]->name,
                                                                  index_[h]->name_size))) {
      h = (h + 1) & index_mask_;
    }
    if (index_[h] == NULL) {
      index_[h] = &defs_.back();
    } else {
      ColumnDefinition* curr = index_[h];
      while(curr->next != NULL) {
        curr = curr->next;
      }
      curr->next = &defs_.back();
    }
  }
}

} // namespace cass
