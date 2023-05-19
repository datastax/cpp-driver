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

#ifndef DATASTAX_INTERNAL_USER_TYPE_ITERATOR_HPP
#define DATASTAX_INTERNAL_USER_TYPE_ITERATOR_HPP

#include "cassandra.h"
#include "data_type.hpp"
#include "iterator.hpp"
#include "serialization.hpp"
#include "value.hpp"

namespace datastax { namespace internal { namespace core {

class UserTypeFieldIterator : public Iterator {
public:
  UserTypeFieldIterator(const Value* user_type_value)
      : Iterator(CASS_ITERATOR_TYPE_USER_TYPE_FIELD)
      , decoder_(user_type_value->decoder()) {
    const UserType& user_type = static_cast<const UserType&>(*user_type_value->data_type());
    next_ = user_type.fields().begin();
    end_ = user_type.fields().end();
  }

  virtual bool next();

  StringRef field_name() const {
    assert(current_ != end_);
    return current_->name;
  }

  const Value* field_value() const {
    assert(current_ != end_);
    return &value_;
  }

private:
  Decoder decoder_;
  UserType::FieldVec::const_iterator next_;
  UserType::FieldVec::const_iterator current_;
  UserType::FieldVec::const_iterator end_;

  Value value_;
};

}}} // namespace datastax::internal::core

#endif
