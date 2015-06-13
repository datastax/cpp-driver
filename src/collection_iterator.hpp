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

#ifndef __CASS_COLLECTION_ITERATOR_HPP_INCLUDED__
#define __CASS_COLLECTION_ITERATOR_HPP_INCLUDED__

#include "cassandra.h"
#include "iterator.hpp"
#include "serialization.hpp"
#include "value.hpp"

namespace cass {

class CollectionIteratorBase : public Iterator {
public:
  CollectionIteratorBase()
    : Iterator(CASS_ITERATOR_TYPE_COLLECTION) { }

  const Value* value() const {
    return &value_;
  }

protected:
  Value value_;
};

class CollectionIterator : public CollectionIteratorBase {
public:
  CollectionIterator(const Value* collection)
      : collection_(collection)
      , position_(collection->data())
      , index_(-1)
      , count_(collection_->value_type() == CASS_VALUE_TYPE_MAP
                   ? (2 * collection_->count())
                   : collection->count()) {}

  virtual bool next();

private:
  char* decode_value(char* position);

private:
  const Value* collection_;

  char* position_;
  int32_t index_;
  const int32_t count_;
};

} // namespace cass

#endif
