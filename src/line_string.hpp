/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_LINE_STRING_HPP_INCLUDED__
#define __DSE_LINE_STRING_HPP_INCLUDED__

#include "dse.h"

#include "serialization.hpp"
#include "wkt.hpp"

#include <external.hpp>

#include <string>

namespace dse {

class LineString {
public:
  LineString() {
    reset();
  }

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
    bytes_.reserve(WKB_HEADER_SIZE +
                   sizeof(cass_uint32_t) +
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

  std::string to_wkt() const;

private:
  cass_uint32_t num_points_;
  Bytes bytes_;
};

class LineStringIterator {
public:
  LineStringIterator()
    : num_points_(0)
    , iterator_(NULL) { }

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
  class Iterator {
  public:
    virtual CassError next_point(cass_double_t *x, cass_double_t *y) = 0;
  };

  class BinaryIterator : public Iterator {
  public:
    BinaryIterator() { }
    BinaryIterator(const cass_byte_t* points_begin,
                   const cass_byte_t* points_end,
                   WkbByteOrder byte_order)
      : position_(points_begin)
      , points_end_(points_end)
      , byte_order_(byte_order) { }

    virtual CassError next_point(cass_double_t *x, cass_double_t *y);

  private:
    const cass_byte_t* position_;
    const cass_byte_t* points_end_;
    WkbByteOrder byte_order_;
  };

  class TextIterator : public Iterator {
  public:
    TextIterator() { }
    TextIterator(const char* text, size_t size);

    virtual CassError next_point(cass_double_t *x, cass_double_t *y);

  private:
    WktLexer lexer_;
  };

  cass_uint32_t num_points_;
  Iterator* iterator_;
  BinaryIterator binary_iterator_;
  TextIterator text_iterator_;
};

} // namespace dse

EXTERNAL_TYPE(dse::LineString, DseLineString)
EXTERNAL_TYPE(dse::LineStringIterator, DseLineStringIterator)

#endif
