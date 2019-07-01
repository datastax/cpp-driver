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

#ifndef DATASTAX_INTERNAL_ABSTRACT_DATA_HPP
#define DATASTAX_INTERNAL_ABSTRACT_DATA_HPP

#include "allocated.hpp"
#include "buffer.hpp"
#include "collection.hpp"
#include "data_type.hpp"
#include "encode.hpp"
#include "hash_table.hpp"
#include "request.hpp"
#include "string_ref.hpp"
#include "types.hpp"
#include "vector.hpp"

#define CASS_CHECK_INDEX_AND_TYPE(Index, Value) \
  do {                                          \
    CassError rc = check(Index, Value);         \
    if (rc != CASS_OK) return rc;               \
  } while (0)

namespace datastax { namespace internal { namespace core {

class Tuple;
class UserTypeValue;

class AbstractData : public Allocated {
public:
  class Element {
  public:
    enum Type { UNSET, NUL, BUFFER, COLLECTION };

    Element()
        : type_(UNSET) {}

    Element(CassNull value)
        : type_(NUL)
        , buf_(core::encode_with_length(value)) {}

    Element(const Buffer& buf)
        : type_(BUFFER)
        , buf_(buf) {}

    Element(const Collection* collection)
        : type_(COLLECTION)
        , collection_(collection) {}

    bool is_unset() const { return type_ == UNSET || (type_ == BUFFER && buf_.size() == 0); }

    bool is_null() const { return type_ == NUL; }

    size_t get_size() const;
    size_t copy_buffer(size_t pos, Buffer* buf) const;
    Buffer get_buffer() const;

  private:
    Type type_;
    Buffer buf_;
    SharedRefPtr<const Collection> collection_;
  };

  typedef Vector<Element> ElementVec;

public:
  AbstractData(size_t count)
      : elements_(count) {}

  virtual ~AbstractData() {}

  const ElementVec& elements() const { return elements_; }

  void reset(size_t count) {
    elements_.clear();
    elements_.resize(count);
  }

#define SET_TYPE(Type)                                  \
  CassError set(size_t index, const Type value) {       \
    CASS_CHECK_INDEX_AND_TYPE(index, value);            \
    elements_[index] = core::encode_with_length(value); \
    return CASS_OK;                                     \
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

  template <class T>
  CassError set(StringRef name, const T value) {
    IndexVec indices;

    if (get_indices(name, &indices) == 0) {
      return CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
    }

    for (IndexVec::const_iterator it = indices.begin(), end = indices.end(); it != end; ++it) {
      size_t index = *it;
      CassError rc = set(index, value);
      if (rc != CASS_OK) return rc;
    }

    return CASS_OK;
  }

  Buffer encode() const;
  Buffer encode_with_length() const;

protected:
  virtual size_t get_indices(StringRef name, IndexVec* indices) = 0;
  virtual const DataType::ConstPtr& get_type(size_t index) const = 0;

private:
  template <class T>
  CassError check(size_t index, const T value) {
    if (index >= elements_.size()) {
      return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }
    IsValidDataType<T> is_valid_type;
    DataType::ConstPtr data_type(get_type(index));
    if (data_type && !is_valid_type(value, data_type)) {
      return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    return CASS_OK;
  }

  size_t get_buffers_size() const;
  void encode_buffers(size_t pos, Buffer* buf) const;

private:
  ElementVec elements_;

private:
  DISALLOW_COPY_AND_ASSIGN(AbstractData);
};

}}} // namespace datastax::internal::core

#endif
