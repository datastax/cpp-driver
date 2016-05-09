/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __DSE_POLYGON_HPP_INCLUDED__
#define __DSE_POLYGON_HPP_INCLUDED__

#include "dse.h"

#include "serialization.hpp"

namespace dse {

class Polygon {
public:
  Polygon() {
    reset();
  }

  const Bytes& bytes() const { return bytes_; }

  void reset() {
    num_rings_ = 0;
    num_points_ = 0;
    ring_start_index_ = 0;
    bytes_.clear();
    bytes_.reserve(WKB_HEADER_SIZE +           // Header
                   sizeof(cass_uint32_t) +     // Num rings
                   sizeof(cass_uint32_t) +     // Num points for one ring
                   6 * sizeof(cass_double_t)); // Simplest ring is 3 points
    encode_header_append(WKB_GEOMETRY_TYPE_POLYGON, bytes_);
    encode_append(0u, bytes_);
  }

  void reserve(cass_uint32_t num_rings, cass_uint32_t total_num_points) {
    bytes_.reserve(WKB_HEADER_SIZE +                        // Header
                   sizeof(cass_uint32_t) +                  // Num rings
                   num_rings * sizeof(cass_uint32_t) +      // Num points for each ring
                   2 * total_num_points * sizeof(cass_double_t)); // Points for each ring
  }

  CassError start_ring() {
    CassError rc = finish_ring(); // Finish the previous ring
    if (rc != CASS_OK) return rc;
    ring_start_index_ = bytes_.size();
    encode_append(0u, bytes_); // Start the ring with zero points
    num_rings_++;
    return CASS_OK;
  }

  void add_point(cass_double_t x, cass_double_t y) {
    encode_append(x, bytes_);
    encode_append(y, bytes_);
    num_points_++;
  }

  CassError finish() {
    if (num_rings_ == 0) {
      return CASS_ERROR_LIB_INVALID_STATE;
    }
    encode(num_rings_, WKB_HEADER_SIZE, bytes_);
    return finish_ring(); // Finish the last ring
  }

private:
  CassError finish_ring() {
    if (ring_start_index_ > 0) {
      if (num_points_ == 1 || num_points_ == 2) {
        return CASS_ERROR_LIB_INVALID_STATE;
      }
      encode(num_points_, ring_start_index_, bytes_);
      num_points_ = 0;
      ring_start_index_ = 0;
    }
    return CASS_OK;
  }

private:
  cass_uint32_t num_rings_;
  cass_uint32_t num_points_;
  size_t ring_start_index_;
  Bytes bytes_;
};

class PolygonIterator {
private:
  enum State {
    STATE_NUM_POINTS,
    STATE_POINTS,
    STATE_DONE
  };

public:
  PolygonIterator()
    : state_(STATE_DONE)
    , rings_(NULL)
    , rings_end_(NULL)
    , position_(NULL)
    , byte_order_(WKB_BYTE_ORDER_LITTLE_ENDIAN)
    , num_rings_(0) { }

  cass_uint32_t num_rings() const { return num_rings_; }
  State state() const { return state_; }

  void reset(cass_uint32_t num_rings,
             const cass_byte_t* rings, const cass_byte_t* rings_end,
             WkbByteOrder byte_order) {
    state_ = STATE_NUM_POINTS;
    rings_ = rings;
    rings_end_ = rings_end;
    position_ = rings;
    points_end_ = NULL;
    byte_order_ = byte_order;
    num_rings_ = num_rings;
  }

   CassError next_num_points(cass_uint32_t* num_points) {
    if (state_ == STATE_NUM_POINTS) {
      uint32_t n = *num_points = decode_uint32(position_, byte_order_);
      position_ += sizeof(cass_uint32_t);
      points_end_ = position_ + n * 2 * sizeof(cass_double_t);
      state_ = STATE_POINTS;
      return CASS_OK;
    }
    return CASS_ERROR_LIB_INVALID_STATE;
  }

  CassError next_point(cass_double_t* x, cass_double_t* y) {
    if (state_ == STATE_POINTS) {
      *x = decode_double(position_, byte_order_);
      position_ += sizeof(cass_double_t);
      *y = decode_double(position_, byte_order_);
      position_ += sizeof(cass_double_t);
      if (position_ >= rings_end_) {
        state_ = STATE_DONE;
      } else if (position_ >= points_end_) {
        state_ = STATE_NUM_POINTS;
      }
      return CASS_OK;
    }
    return CASS_ERROR_LIB_INVALID_STATE;
  }

private:
  State state_;
  const cass_byte_t* rings_;
  const cass_byte_t* rings_end_;
  const cass_byte_t* position_;
  const cass_byte_t* points_end_;
  WkbByteOrder byte_order_;
  cass_uint32_t num_rings_;
};

} // namespace dse

#endif
