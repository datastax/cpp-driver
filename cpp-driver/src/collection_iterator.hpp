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

#ifndef DATASTAX_INTERNAL_VALUE_ITERATOR_HPP
#define DATASTAX_INTERNAL_VALUE_ITERATOR_HPP

#include "cassandra.h"
#include "iterator.hpp"
#include "serialization.hpp"
#include "value.hpp"

namespace datastax { namespace internal { namespace core {

class ValueIterator : public Iterator {
public:
  ValueIterator(CassIteratorType type, Decoder decoder)
      : Iterator(type)
      , decoder_(decoder) {}

  const Value* value() const { return &value_; }

protected:
  Decoder decoder_;
  Value value_;
};

class CollectionIterator : public ValueIterator {
public:
  CollectionIterator(const Value* collection)
      : ValueIterator(CASS_ITERATOR_TYPE_COLLECTION, collection->decoder())
      , collection_(collection)
      , index_(-1)
      , count_(collection_->value_type() == CASS_VALUE_TYPE_MAP ? (2 * collection_->count())
                                                                : collection->count()) {}

  virtual bool next();

private:
  bool decode_value();

private:
  const Value* collection_;

  int32_t index_;
  const int32_t count_;
};

class TupleIterator : public ValueIterator {
public:
  TupleIterator(const Value* tuple)
      : ValueIterator(CASS_ITERATOR_TYPE_TUPLE, tuple->decoder()) {
    CollectionType::ConstPtr collection_type(tuple->data_type());
    next_ = collection_type->types().begin();
    end_ = collection_type->types().end();
  }

  virtual bool next();

private:
  DataType::Vec::const_iterator next_;
  DataType::Vec::const_iterator current_;
  DataType::Vec::const_iterator end_;
};

}}} // namespace datastax::internal::core

#endif
