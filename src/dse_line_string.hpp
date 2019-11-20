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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_LINE_STRING_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_LINE_STRING_HPP

#include "dse.h"

#include "allocated.hpp"
#include "dse_serialization.hpp"
#include "string.hpp"
#include "wkt.hpp"

#include "external.hpp"

namespace datastax { namespace internal { namespace enterprise {

class LineString : public Allocated {
public:
  LineString() { reset(); }

  const Bytes& bytes() const { return bytes_; }

  void reset() {
    num_points_ = 0;
    bytes_.clear();
    bytes_.reserve(WKB_HEADER_SIZE +           // Header
                   sizeof(cass_uint32_t) +     // Num points
                   4 * sizeof(cass_double_t)); // Simplest linestring is 2 points
    encode_header_append(WKB_GEOMETRY_TYPE_LINESTRING, bytes_);
    encode_append(0u, bytes_);
  }

  void reserve(cass_uint32_t num_points) {
    bytes_.reserve(WKB_HEADER_SIZE + sizeof(cass_uint32_t) +
                   2 * num_points * sizeof(cass_double_t));
  }

  void add_point(cass_double_t x, cass_double_t y) {
    encode_append(x, bytes_);
    encode_append(y, bytes_);
    num_points_++;
  }

  CassError finish() {
    if (num_points_ == 1) {
      return CASS_ERROR_LIB_INVALID_STATE;
    }
    encode(num_points_, WKB_HEADER_SIZE, bytes_);
    return CASS_OK;
  }

  datastax::String to_wkt() const;

private:
  cass_uint32_t num_points_;
  Bytes bytes_;
};

class LineStringIterator : public Allocated {
public:
  LineStringIterator()
      : num_points_(0)
      , iterator_(NULL) {}

  cass_uint32_t num_points() const { return num_points_; }

  CassError reset_binary(const CassValue* value);
  CassError reset_text(const char* text, size_t size);

  CassError next_point(cass_double_t* x, cass_double_t* y) {
    if (iterator_ == NULL) {
      return CASS_ERROR_LIB_INVALID_STATE;
    }
    return iterator_->next_point(x, y);
  }

private:
  class Iterator : public Allocated {
  public:
    virtual ~Iterator() {}
    virtual CassError next_point(cass_double_t* x, cass_double_t* y) = 0;
  };

  class BinaryIterator : public Iterator {
  public:
    BinaryIterator() {}
    BinaryIterator(const cass_byte_t* points_begin, const cass_byte_t* points_end,
                   WkbByteOrder byte_order)
        : position_(points_begin)
        , points_end_(points_end)
        , byte_order_(byte_order) {}

    virtual CassError next_point(cass_double_t* x, cass_double_t* y);

  private:
    const cass_byte_t* position_;
    const cass_byte_t* points_end_;
    WkbByteOrder byte_order_;
  };

  class TextIterator : public Iterator {
  public:
    TextIterator() {}
    TextIterator(const char* text, size_t size);

    virtual CassError next_point(cass_double_t* x, cass_double_t* y);

  private:
    WktLexer lexer_;
  };

  cass_uint32_t num_points_;
  Iterator* iterator_;
  BinaryIterator binary_iterator_;
  TextIterator text_iterator_;
};

}}} // namespace datastax::internal::enterprise

EXTERNAL_TYPE(datastax::internal::enterprise::LineString, DseLineString)
EXTERNAL_TYPE(datastax::internal::enterprise::LineStringIterator, DseLineStringIterator)

#endif
