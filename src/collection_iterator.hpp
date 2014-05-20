/*
  Copyright (c) 2014 DataStax

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
      : Iterator(CASS_ITERATOR_COLLECTION)
      , collection_(collection)
      , position_(collection->buffer.data())
      , index_(0) { }


    char* decode_value(char* position) {
      uint16_t size;
      char* buffer = decode_short(position, size);

      CassValueType type;
      if(collection_->type == CASS_VALUE_TYPE_MAP) {
        type = (index_ % 2 == 0) ? collection_->primary_type : collection_->secondary_type;
      } else {
        type = collection_->primary_type;
      }

      value_ = Value(type, buffer, size);

      return buffer + size;
    }

    virtual bool next() {
      if(index_++ < collection_->count) {
        position_ = decode_value(position_);
        return true;
      }
      return false;
    }

    const Value* value() { return &value_; }

  private:
    const Value* collection_;
    char* position_;
    Value value_;
    size_t index_;
};

} // namespace cass

#endif
