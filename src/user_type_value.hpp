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

#ifndef DATASTAX_INTERNAL_USER_TYPE_VALUE_HPP
#define DATASTAX_INTERNAL_USER_TYPE_VALUE_HPP

#include "abstract_data.hpp"
#include "cassandra.h"
#include "data_type.hpp"
#include "external.hpp"
#include "ref_counted.hpp"

namespace datastax { namespace internal { namespace core {

class UserTypeValue : public AbstractData {
public:
  UserTypeValue(const UserType::ConstPtr& data_type)
      : AbstractData(data_type->fields().size())
      , data_type_(data_type) {}

  const UserType::ConstPtr& data_type() const { return data_type_; }

protected:
  virtual size_t get_indices(StringRef name, IndexVec* indices) {
    return data_type_->get_indices(name, indices);
  }

  virtual const DataType::ConstPtr& get_type(size_t index) const {
    return data_type_->fields()[index].type;
  }

private:
  UserType::ConstPtr data_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(UserTypeValue);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::UserTypeValue, CassUserType)

#endif
