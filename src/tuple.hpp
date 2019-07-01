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

#ifndef DATASTAX_INTERNAL_TUPLE_HPP
#define DATASTAX_INTERNAL_TUPLE_HPP

#include "allocated.hpp"
#include "buffer.hpp"
#include "cassandra.h"
#include "data_type.hpp"
#include "encode.hpp"
#include "external.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#define CASS_TUPLE_CHECK_INDEX_AND_TYPE(Index, Value) \
  do {                                                \
    CassError rc = check(Index, Value);               \
    if (rc != CASS_OK) return rc;                     \
  } while (0)

namespace datastax { namespace internal { namespace core {

class Collection;
class UserTypeValue;

class Tuple : public Allocated {
public:
  explicit Tuple(size_t item_count)
      : data_type_(new TupleType(false))
      , items_(item_count) {}

  explicit Tuple(const DataType::ConstPtr& data_type)
      : data_type_(data_type)
      , items_(data_type_->types().size()) {}

  const TupleType::ConstPtr& data_type() const { return data_type_; }
  const BufferVec& items() const { return items_; }

#define SET_TYPE(Type)                               \
  CassError set(size_t index, const Type value) {    \
    CASS_TUPLE_CHECK_INDEX_AND_TYPE(index, value);   \
    items_[index] = core::encode_with_length(value); \
    return CASS_OK;                                  \
  }

  SET_TYPE(cass_int8_t)
  SET_TYPE(cass_int16_t)
  SET_TYPE(cass_int32_t)
  SET_TYPE(cass_uint32_t)
  SET_TYPE(cass_int64_t)
  SET_TYPE(cass_float_t)
  SET_TYPE(cass_double_t)
  SET_TYPE(cass_bool_t)
  SET_TYPE(CassString)
  SET_TYPE(CassBytes)
  SET_TYPE(CassCustom)
  SET_TYPE(CassUuid)
  SET_TYPE(CassInet)
  SET_TYPE(CassDecimal)
  SET_TYPE(CassDuration)

#undef SET_TYPE

  CassError set(size_t index, CassNull value);
  CassError set(size_t index, const Collection* value);
  CassError set(size_t index, const Tuple* value);
  CassError set(size_t index, const UserTypeValue* value);

  Buffer encode() const;
  Buffer encode_with_length() const;

private:
  template <class T>
  CassError check(size_t index, const T value) {
    if (index > items_.size()) {
      return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    IsValidDataType<T> is_valid_type;
    if (index < data_type()->types().size() && !is_valid_type(value, data_type_->types()[index])) {
      return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    return CASS_OK;
  }

  size_t get_buffers_size() const;
  void encode_buffers(size_t pos, Buffer* buf) const;

private:
  TupleType::ConstPtr data_type_;
  BufferVec items_;

private:
  DISALLOW_COPY_AND_ASSIGN(Tuple);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Tuple, CassTuple)

#endif
