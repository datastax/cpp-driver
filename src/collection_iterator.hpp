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
#include "value.hpp"
#include "serialization.hpp"

namespace cass {

class CollectionIterator : public Iterator {
public:
  CollectionIterator(const Value* collection)
      : Iterator(CASS_ITERATOR_TYPE_COLLECTION)
      , collection_(collection)
      , position_(collection->buffer().data())
      , index_(-1)
      , count_(collection_->type() == CASS_VALUE_TYPE_MAP
                   ? (2 * collection_->count())
                   : collection->count()) {}

  virtual bool next();

  const Value* value() {
    assert(index_ >= 0 && index_ < count_);
    return &value_;
  }

private:
  char* decode_value(char* position);

private:
  const Value* collection_;
  char* position_;
  Value value_;
  int32_t index_;
  const int32_t count_;
};

} // namespace cass

#endif
