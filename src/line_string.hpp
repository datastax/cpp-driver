/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_LINE_STRING_HPP_INCLUDED__
#define __DSE_LINE_STRING_HPP_INCLUDED__

#include "dse.h"

#include "serialization.hpp"

namespace dse {

class LineSegment {
public:
  LineSegment() {
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

private:
  cass_uint32_t num_points_;
  Bytes bytes_;
};

class LineSegmentIterator {
public:
  LineSegmentIterator()
    : position_(NULL)
    , points_end_(NULL)
    , byte_order_(WKB_BYTE_ORDER_LITTLE_ENDIAN)
    , num_points_(0) { }

  cass_uint32_t num_points() const { return num_points_; }

  void reset(cass_uint32_t num_points,
             const cass_byte_t* points_begin,
             const cass_byte_t* points_end,
             WkbByteOrder byte_order) {
    num_points_ = num_points;
    position_ = points_begin;
    points_end_ = points_end;
    byte_order_ = byte_order;
  }

  CassError next_point(cass_double_t* x, cass_double_t* y) {
    if (position_ < points_end_) {
      *x = decode_double(position_, byte_order_);
      position_ += sizeof(cass_double_t);
      *y = decode_double(position_, byte_order_);
      position_ += sizeof(cass_double_t);
      return CASS_OK;
    }
    return CASS_ERROR_LIB_INVALID_STATE;
  }

private:
  const cass_byte_t* position_;
  const cass_byte_t* points_end_;
  WkbByteOrder byte_order_;
  cass_uint32_t num_points_;
};

} // namespace dse

#endif
