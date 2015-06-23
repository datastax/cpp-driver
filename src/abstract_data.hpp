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

#ifndef __CASS_ABSTRACT_DATA_HPP_INCLUDED__
#define __CASS_ABSTRACT_DATA_HPP_INCLUDED__

#include "buffer.hpp"
#include "collection.hpp"
#include "data_type.hpp"
#include "encode.hpp"
#include "hash_index.hpp"
#include "request.hpp"
#include "string_ref.hpp"
#include "types.hpp"

#define CASS_CHECK_INDEX_AND_TYPE(Index, Value) do { \
  CassError rc = check(Index, Value);               \
  if (rc != CASS_OK) return rc;                     \
} while(0)

namespace cass {

class AbstractData {
public:
  class Element {
  public:
    enum Type {
      EMPTY,
      BUFFER,
      COLLECTION
    };

    Element()
      : type_(EMPTY) { }

    Element(const Buffer& buf)
      : type_(BUFFER)
      , buf_(buf) { }

    Element(const Collection* collection)
      : type_(COLLECTION)
      , collection_(collection) { }

    bool is_empty() const {
      return type_ == EMPTY || (type_ == BUFFER && buf_.size() == 0);
    }

    size_t get_size(int version) const {
      if (type_ == COLLECTION) {
        return collection_->get_items_size(version);
      } else {
        assert(type_ == BUFFER);
        return buf_.size();
      }
    }

    Buffer get_buffer(int version) const {
      if (type_ == COLLECTION) {
        return collection_->encode_with_length(version);
      } else {
        assert(type_ == BUFFER);
        return buf_;
      }
    }

    Buffer get_buffer_cached(int version, Request::EncodingCache* cache, bool add_to_cache) const {
      if (type_ == COLLECTION) {
        Request::EncodingCache::const_iterator i = cache->find(collection_.get());
        if (i != cache->end()) {
          return i->second;
        } else {
          Buffer buf(collection_->encode_with_length(version));
          if (add_to_cache) {
            // TODO: Is there a size threshold where it might be faster to alway re-encode?
            cache->insert(std::make_pair(collection_.get(), buf));
          }
          return buf;
        }
      } else {
        assert(type_ == BUFFER);
        return buf_;
      }
    }

  private:
    Type type_;
    Buffer buf_;
    SharedRefPtr<const Collection> collection_;
  };

  typedef std::vector<Element> ElementVec;

public:
  AbstractData(size_t count)
    : elements_(count) { }

  virtual ~AbstractData() { }

  const ElementVec& elements() const { return elements_; }
  size_t elements_count() const { return elements_.size(); }

#define SET_TYPE(Type)                                  \
  CassError set(size_t index, const Type value) {       \
    CASS_CHECK_INDEX_AND_TYPE(index, value);            \
    elements_[index] = cass::encode_with_length(value); \
    return CASS_OK;                                     \
  }

  SET_TYPE(CassNull)
  SET_TYPE(cass_int32_t)
  SET_TYPE(cass_int64_t)
  SET_TYPE(cass_float_t)
  SET_TYPE(cass_double_t)
  SET_TYPE(cass_bool_t)
  SET_TYPE(CassString)
  SET_TYPE(CassBytes)
  SET_TYPE(CassUuid)
  SET_TYPE(CassInet)
  SET_TYPE(CassDecimal)

#undef SET_TYPE

  CassError set(size_t index, CassCustom custom);
  CassError set(size_t index, const Collection* value);

  template<class T>
  CassError set(StringRef name, const T value) {
    cass::HashIndex::IndexVec indices;

    if (get_indices(name, &indices) == 0) {
      return CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
    }

    for (cass::HashIndex::IndexVec::const_iterator it = indices.begin(),
         end = indices.end(); it != end; ++it) {
      size_t index = *it;
      CassError rc = set(index, value);
      if (rc != CASS_OK) return rc;
    }

    return CASS_OK;
  }

  Buffer encode_with_length() const;

  int32_t copy_buffers(int version, BufferVec* bufs, Request::EncodingCache* cache) const;

protected:
  virtual size_t get_indices(StringRef name,
                             HashIndex::IndexVec* indices) const = 0;
  virtual const SharedRefPtr<DataType>& get_type(size_t index) const = 0;

private:
  template <class T>
  CassError check(size_t index, const T value) {
    if (index >= elements_.size()) {
      return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }
    IsValidDataType<T> is_valid_type;
    const SharedRefPtr<DataType> data_type(get_type(index));
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

} // namespace cass

#endif
