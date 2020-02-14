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

#ifndef DATASTAX_INTERNAL_HASH_INDEX_HPP
#define DATASTAX_INTERNAL_HASH_INDEX_HPP

#include "allocated.hpp"
#include "hash.hpp"
#include "macros.hpp"
#include "small_vector.hpp"
#include "string_ref.hpp"
#include "utils.hpp"

#include <stdint.h>

// This can be decreased to reduce hash collisions, but it will require
// additional memory.
#define CASS_LOAD_FACTOR 0.75

namespace datastax { namespace internal { namespace core {

typedef SmallVector<size_t, 4> IndexVec;

template <class T>
struct HashTableEntry {
  HashTableEntry()
      : index(0)
      , next(NULL) {}

  // Requires a "name" String or datastax::StringRef field
  size_t index;
  T* next;
};

template <class T>
class CaseInsensitiveHashTable : public Allocated {
public:
  typedef SmallVector<T, 16> EntryVec;

  CaseInsensitiveHashTable(size_t capacity = 16);
  CaseInsensitiveHashTable(const EntryVec& entries);

  T& operator[](size_t index) { return entries_[index]; }
  const T& operator[](size_t index) const { return entries_[index]; }

  size_t get_indices(StringRef name, IndexVec* result) const;
  size_t add(const T& entry);

  const EntryVec& entries() const { return entries_; }
  void set_entries(const EntryVec& entries);

  size_t size() const { return entries_.size(); }

private:
  void add_index(T* entry);
  void reset(size_t capacity);
  void resize(size_t new_capacity);
  void reindex();

private:
  size_t index_mask_;
  SmallVector<T*, 32> index_;
  EntryVec entries_;

private:
  DISALLOW_COPY_AND_ASSIGN(CaseInsensitiveHashTable);
};

template <class T>
CaseInsensitiveHashTable<T>::CaseInsensitiveHashTable(size_t capacity) {
  reset(capacity);
}

template <class T>
CaseInsensitiveHashTable<T>::CaseInsensitiveHashTable(const EntryVec& entries) {
  set_entries(entries);
}

template <class T>
size_t CaseInsensitiveHashTable<T>::get_indices(StringRef name, IndexVec* result) const {
  result->clear();
  bool is_case_sensitive = false;

  if (!name.data()) {
    return 0;
  }

  if (name.size() > 0 && name.front() == '"' && name.back() == '"') {
    is_case_sensitive = true;
    name = name.substr(1, name.size() - 2);
  }

  size_t h = hash::fnv1a(name.data(), name.size(), ::tolower) & index_mask_;

  size_t start = h;
  while (index_[h] != NULL && !iequals(name, index_[h]->name)) {
    h = (h + 1) & index_mask_;
    if (h == start) {
      return 0;
    }
  }

  const T* entry = index_[h];

  if (entry == NULL) {
    return 0;
  }

  if (!is_case_sensitive) {
    while (entry != NULL) {
      result->push_back(entry->index);
      entry = entry->next;
    }
  } else {
    while (entry != NULL) {
      if (name.equals(entry->name)) {
        result->push_back(entry->index);
      }
      entry = entry->next;
    }
  }

  return result->size();
}

template <class T>
size_t CaseInsensitiveHashTable<T>::add(const T& entry) {
  size_t index = entries_.size();
  size_t capacity = entries_.capacity();
  if (index >= capacity) {
    resize(2 * capacity);
  }
  entries_.push_back(entry);
  T* back = &entries_.back();
  back->index = index;
  add_index(back);
  return index;
}

template <class T>
void CaseInsensitiveHashTable<T>::set_entries(const EntryVec& entries) {
  entries_.clear();
  reset(entries.size());
  for (size_t i = 0; i < entries.size(); ++i) {
    add(entries[i]);
  }
}

template <class T>
void CaseInsensitiveHashTable<T>::add_index(T* entry) {
  size_t h = hash::fnv1a(entry->name.data(), entry->name.size(), ::tolower) & index_mask_;

  if (index_[h] == NULL) {
    index_[h] = entry;
  } else {
    // Use linear probing to find an open bucket
    size_t start = h;
    while (index_[h] != NULL && !iequals(entry->name, index_[h]->name)) {
      h = (h + 1) & index_mask_;
      if (h == start) {
        return;
      }
    }
    if (index_[h] == NULL) {
      index_[h] = entry;
    } else {
      T* curr = index_[h];
      while (curr->next != NULL) {
        curr = curr->next;
      }
      curr->next = entry;
    }
  }
}

template <class T>
void CaseInsensitiveHashTable<T>::reset(size_t capacity) {
  if (capacity < entries_.capacity()) {
    capacity = entries_.capacity();
  }
  size_t index_capacity = next_pow_2(static_cast<size_t>(capacity / CASS_LOAD_FACTOR) + 1);
  std::fill(index_.begin(), index_.end(), static_cast<T*>(NULL)); // Clear the old entries
  index_.resize(index_capacity);
  entries_.reserve(capacity);
  index_mask_ = index_capacity - 1;
}

template <class T>
void CaseInsensitiveHashTable<T>::resize(size_t new_capacity) {
  reset(new_capacity);
  reindex();
}

template <class T>
void CaseInsensitiveHashTable<T>::reindex() {
  for (size_t i = 0; i < entries_.size(); ++i) {
    T* entry = &entries_[i];
    entry->index = i;
    add_index(entry);
  }
}

}}} // namespace datastax::internal::core

#endif
