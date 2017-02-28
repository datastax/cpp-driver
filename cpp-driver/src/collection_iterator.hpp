/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_VALUE_ITERATOR_HPP_INCLUDED__
#define __CASS_VALUE_ITERATOR_HPP_INCLUDED__

#include "cassandra.h"
#include "iterator.hpp"
#include "serialization.hpp"
#include "value.hpp"

namespace cass {

class ValueIterator : public Iterator {
public:
  ValueIterator(CassIteratorType type)
    : Iterator(type) { }

  const Value* value() const {
    return &value_;
  }

protected:
  Value value_;
};

class CollectionIterator : public ValueIterator {
public:
  CollectionIterator(const Value* collection)
      : ValueIterator(CASS_ITERATOR_TYPE_COLLECTION)
      , collection_(collection)
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

class TupleIterator : public ValueIterator {
public:
  TupleIterator(const Value* tuple)
      : ValueIterator(CASS_ITERATOR_TYPE_TUPLE)
      , tuple_(tuple)
      , position_(tuple->data()) {
    CollectionType::ConstPtr collection_type(tuple->data_type());
    next_ = collection_type->types().begin();
    end_ = collection_type->types().end();
  }

  virtual bool next();

private:
  char* decode_value(char* position);

private:
  const Value* tuple_;

  char* position_;
  DataType::Vec::const_iterator next_;
  DataType::Vec::const_iterator current_;
  DataType::Vec::const_iterator end_;
};

} // namespace cass

#endif
