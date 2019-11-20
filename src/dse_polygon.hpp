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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_POLYGON_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_POLYGON_HPP

#include "dse.h"

#include "allocated.hpp"
#include "dse_serialization.hpp"
#include "string.hpp"
#include "wkt.hpp"

#include "external.hpp"

namespace datastax { namespace internal { namespace enterprise {

class Polygon : public Allocated {
public:
  Polygon() { reset(); }

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
    bytes_.reserve(WKB_HEADER_SIZE +                              // Header
                   sizeof(cass_uint32_t) +                        // Num rings
                   num_rings * sizeof(cass_uint32_t) +            // Num points for each ring
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
    encode(num_rings_, WKB_HEADER_SIZE, bytes_);
    return finish_ring(); // Finish the last ring
  }

  datastax::String to_wkt() const;

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

class PolygonIterator : public Allocated {
private:
  enum State { STATE_NUM_POINTS, STATE_POINTS, STATE_DONE };

public:
  PolygonIterator()
      : num_rings_(0)
      , iterator_(NULL) {}

  cass_uint32_t num_rings() const { return num_rings_; }

  CassError reset_binary(const CassValue* value);
  CassError reset_text(const char* text, size_t size);

  CassError next_num_points(cass_uint32_t* num_points) {
    if (iterator_ == NULL) {
      return CASS_ERROR_LIB_INVALID_STATE;
    }
    return iterator_->next_num_points(num_points);
  }

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
    virtual CassError next_num_points(cass_uint32_t* num_points) = 0;
    virtual CassError next_point(cass_double_t* x, cass_double_t* y) = 0;
  };

  class BinaryIterator : public Iterator {
  public:
    BinaryIterator() {}
    BinaryIterator(const cass_byte_t* rings_begin, const cass_byte_t* rings_end,
                   WkbByteOrder byte_order)
        : state_(STATE_NUM_POINTS)
        , position_(rings_begin)
        , rings_end_(rings_end)
        , points_end_(NULL)
        , byte_order_(byte_order) {}

    virtual CassError next_num_points(cass_uint32_t* num_points);
    virtual CassError next_point(cass_double_t* x, cass_double_t* y);

  private:
    State state_;
    const cass_byte_t* position_;
    const cass_byte_t* rings_end_;
    const cass_byte_t* points_end_;
    WkbByteOrder byte_order_;
  };

  class TextIterator : public Iterator {
  public:
    TextIterator() {}
    TextIterator(const char* text, size_t size);

    virtual CassError next_num_points(cass_uint32_t* num_points);
    virtual CassError next_point(cass_double_t* x, cass_double_t* y);

  private:
    State state_;
    WktLexer lexer_;
  };

  cass_uint32_t num_rings_;
  Iterator* iterator_;
  BinaryIterator binary_iterator_;
  TextIterator text_iterator_;
};

}}} // namespace datastax::internal::enterprise

EXTERNAL_TYPE(datastax::internal::enterprise::Polygon, DsePolygon)
EXTERNAL_TYPE(datastax::internal::enterprise::PolygonIterator, DsePolygonIterator)

#endif
