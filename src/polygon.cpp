/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "polygon.hpp"
#include "validate.hpp"

#include <assert.h>
#include <iomanip>
#include <sstream>

extern "C" {

DsePolygon* dse_polygon_new() {
  return DsePolygon::to(new dse::Polygon());
}

void dse_polygon_free(DsePolygon* polygon) {
  delete polygon->from();
}

void dse_polygon_reset(DsePolygon* polygon) {
  polygon->reset();
}

void dse_polygon_reserve(DsePolygon* polygon,
                         cass_uint32_t num_rings,
                         cass_uint32_t total_num_points) {
  polygon->reserve(num_rings, total_num_points);
}

CassError dse_polygon_start_ring(DsePolygon* polygon) {
  return polygon->start_ring();
}

CassError dse_polygon_add_point(DsePolygon* polygon,
                                cass_double_t x, cass_double_t y) {
  polygon->add_point(x, y);
  return CASS_OK;
}

CassError dse_polygon_finish(DsePolygon* polygon) {
  return polygon->finish();
}

DsePolygonIterator* dse_polygon_iterator_new() {
  return DsePolygonIterator::to(new dse::PolygonIterator());
}

CassError dse_polygon_iterator_reset(DsePolygonIterator* iterator,
                                     const CassValue* value) {
  return iterator->reset_binary(value);
}

CassError dse_polygon_text_iterator_reset_n(DsePolygonIterator* iterator,
                                            const char* value,
                                            size_t value_length) {
  return iterator->reset_text(value, value_length);
}

CassError dse_polygon_text_iterator_reset(DsePolygonIterator* iterator,
                                          const char* value) {
  return dse_polygon_text_iterator_reset_n(iterator, value, strlen(value));
}

void dse_polygon_iterator_free(DsePolygonIterator* iterator) {
  delete iterator->from();
}

cass_uint32_t dse_polygon_iterator_num_rings(const DsePolygonIterator* iterator) {
  return iterator->num_rings();
}

CassError dse_polygon_iterator_next_num_points(DsePolygonIterator* iterator,
                                               cass_uint32_t* num_points) {
  return iterator->next_num_points(num_points);
}

CassError dse_polygon_iterator_next_point(DsePolygonIterator* iterator,
                                          cass_double_t* x, cass_double_t* y) {
  return iterator->next_point(x, y);
}

} // extern "C"

namespace dse {

std::string Polygon::to_wkt() const {
  std::stringstream ss;
  ss.precision(WKT_MAX_DIGITS);
  ss << "POLYGON (";
  const cass_byte_t* pos = bytes_.data() + WKB_HEADER_SIZE + sizeof(cass_uint32_t);
  for (cass_uint32_t i = 0; i < num_rings_; ++i) {
    if (i > 0) ss << ", ";
    ss << "(";
    cass_uint32_t num_points = decode_uint32(pos, native_byte_order());
    pos += sizeof(cass_uint32_t);
    for (cass_uint32_t j = 0; j < num_points; ++j) {
      if (j > 0) ss << ", ";
      ss << decode_double(pos, native_byte_order());
      pos += sizeof(cass_double_t);
      ss << " ";
      ss << decode_double(pos, native_byte_order());
      pos += sizeof(cass_double_t);
    }
    ss << ")";
  }
  ss << ")";
  return ss.str();
}

CassError PolygonIterator::reset_binary(const CassValue* value) {
  size_t size;
  const cass_byte_t* pos;
  dse::WkbByteOrder byte_order;
  cass_uint32_t num_rings;

  CassError rc = dse::validate_data_type(value, DSE_POLYGON_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_POLYGON_HEADER_SIZE) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }
  size -= WKB_POLYGON_HEADER_SIZE;

  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_POLYGON) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  num_rings = dse::decode_uint32(pos, byte_order);
  pos += sizeof(cass_uint32_t);

  const cass_byte_t* rings = pos;
  const cass_byte_t* rings_end = pos + size;

  for (cass_uint32_t i = 0; i < num_rings; ++i) {
    cass_uint32_t num_points;

    if (size < sizeof(cass_uint32_t)) {
      return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    }
    size -= sizeof(cass_uint32_t);

    num_points = dse::decode_uint32(pos, byte_order);
    pos += sizeof(cass_uint32_t);

    if (size < 2 * num_points * sizeof(cass_double_t)) {
      return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    }
    size -= 2 * num_points * sizeof(cass_double_t);
  }

  num_rings_ = num_rings;
  binary_iterator_ = BinaryIterator(rings, rings_end, byte_order);
  iterator_ = &binary_iterator_;

  return CASS_OK;
}

CassError PolygonIterator::reset_text(const char* text, size_t size) {
  cass_uint32_t num_rings = 0;
  const bool skip_numbers = true;
  WktLexer lexer(text, size, skip_numbers);

  if (lexer.next_token() != WktLexer::TK_TYPE_POLYGON) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  // Validate format and count the number of rings
  if (lexer.next_token() != WktLexer::TK_OPEN_PAREN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  WktLexer::Token token = lexer.next_token();
  while (token != WktLexer::TK_EOF && token != WktLexer::TK_CLOSE_PAREN) {
    // Start ring
    if (token != WktLexer::TK_OPEN_PAREN) {
      return CASS_ERROR_LIB_BAD_PARAMS;
    }

    // Consume points in ring
    token = lexer.next_token();
    while (token != WktLexer::TK_EOF && token != WktLexer::TK_CLOSE_PAREN) {
      // First number in point
      if (token != WktLexer::TK_NUMBER) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }

      // Second number in point
      token = lexer.next_token();
      if (token != WktLexer::TK_NUMBER) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }

      // Check and skip "," token
      token = lexer.next_token();
      if (token == WktLexer::TK_COMMA) {
        token = lexer.next_token();
        // Verify there are more points
        if(token != WktLexer::TK_NUMBER) {
          return CASS_ERROR_LIB_BAD_PARAMS;
        }
      }
    }

    // End ring
    if (token != WktLexer::TK_CLOSE_PAREN) {
      return CASS_ERROR_LIB_BAD_PARAMS;
    }

    ++num_rings;

    // Check and skip "," token
    token = lexer.next_token();
    if (token == WktLexer::TK_COMMA) {
      token = lexer.next_token();
      // Verify there are more rings
      if (token != WktLexer::TK_OPEN_PAREN) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }
    }
  }

  // Validate closing ")"
  if (token != WktLexer::TK_CLOSE_PAREN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  num_rings_ = num_rings;
  text_iterator_ = TextIterator(text, size);
  iterator_ = &text_iterator_;

  return CASS_OK;
}

CassError PolygonIterator::BinaryIterator::next_num_points(cass_uint32_t* num_points) {
  if (state_ != STATE_NUM_POINTS) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }

  uint32_t n = *num_points = decode_uint32(position_, byte_order_);
  position_ += sizeof(cass_uint32_t);
  points_end_ = position_ + n * 2 * sizeof(cass_double_t);
  state_ = STATE_POINTS;

  return CASS_OK;
}

CassError PolygonIterator::BinaryIterator::next_point(cass_double_t* x, cass_double_t* y) {
  if (state_ != STATE_POINTS) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }

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

PolygonIterator::TextIterator::TextIterator(const char* text, size_t size)
  : state_(STATE_NUM_POINTS)
  , lexer_(text, size) {
  WktLexer::Token token; UNUSED_(token);
  // Skip over "POLYGON (" tokens
  token = lexer_.next_token(); assert(token == WktLexer::TK_TYPE_POLYGON);
  token = lexer_.next_token(); assert(token == WktLexer::TK_OPEN_PAREN);
}

CassError PolygonIterator::TextIterator::next_num_points(cass_uint32_t* num_points) {
  WktLexer::Token token;
  token = lexer_.next_token();

  *num_points = 0;

  // If we're at the end of the text this will return WktLexer::TK_EOF
  if (state_ != STATE_NUM_POINTS || token != WktLexer::TK_OPEN_PAREN) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }

  const bool skip_numbers = true;
  WktLexer temp(lexer_, skip_numbers); // Read forward using a temp lexer

  token = temp.next_token();
  while (token != WktLexer::TK_EOF && token != WktLexer::TK_CLOSE_PAREN) {
    // First number in point
    assert(token == WktLexer::TK_NUMBER);

    // Second number in point
    token = temp.next_token(); assert(token == WktLexer::TK_NUMBER);

    ++(*num_points);

    // Check and skip "," token
    token = temp.next_token();
    if (token == WktLexer::TK_COMMA) {
      token = temp.next_token();
      assert(token == WktLexer::TK_NUMBER);
    }
  }

  assert(token == WktLexer::TK_CLOSE_PAREN);
  state_ = STATE_POINTS;

  return CASS_OK;
}

CassError PolygonIterator::TextIterator::next_point(cass_double_t* x, cass_double_t* y) {
  WktLexer::Token token;

  if (state_ != STATE_POINTS) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }

  // First number in point
  token = lexer_.next_token(); assert(token == WktLexer::TK_NUMBER);
  *x = lexer_.number();

  // Second number in point
  token = lexer_.next_token(); assert(token == WktLexer::TK_NUMBER);
  *y = lexer_.number();

  token = lexer_.next_token();
  if (token == WktLexer::TK_CLOSE_PAREN) {
     // Done with this ring
    token = lexer_.next_token();
    if (token == WktLexer::TK_CLOSE_PAREN) {
      // Done with last ring
      state_ = STATE_DONE;
    } else {
      // More rings
      assert(token == WktLexer::TK_COMMA);
      state_ = STATE_NUM_POINTS;
    }
  } else {
    // More points
    assert(token == WktLexer::TK_COMMA);
  }

  return CASS_OK;
}

} // namespace dse
