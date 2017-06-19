/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "line_string.hpp"
#include "memory.hpp"
#include "validate.hpp"

#include <string_ref.hpp>

#include <assert.h>
#include <iomanip>
#include <sstream>

extern "C" {

DseLineString* dse_line_string_new() {
  return DseLineString::to(cass::Memory::allocate<dse::LineString>());
}

void dse_line_string_free(DseLineString* line_string) {
  cass::Memory::deallocate(line_string->from());
}

void dse_line_string_reset(DseLineString* line_string) {
  line_string->reset();
}

void dse_line_string_reserve(DseLineString* line_string,
                             cass_uint32_t num_points) {
  line_string->reserve(num_points);
}

CassError dse_line_string_add_point(DseLineString* line_string,
                                    cass_double_t x, cass_double_t y) {
  line_string->add_point(x, y);
  return CASS_OK;
}

CassError dse_line_string_finish(DseLineString* line_string) {
  return line_string->finish();
}

DseLineStringIterator* dse_line_string_iterator_new() {
  return DseLineStringIterator::to(cass::Memory::allocate<dse::LineStringIterator>());
}

void dse_line_string_iterator_free(DseLineStringIterator* iterator) {
  cass::Memory::deallocate(iterator->from());
}

CassError dse_line_string_iterator_reset(DseLineStringIterator *iterator, const CassValue *value) {
  return iterator->reset_binary(value);
}

CassError dse_line_string_iterator_reset_with_wkt_n(DseLineStringIterator* iterator,
                                                    const char* wkt,
                                                    size_t wkt_length) {
  return iterator->reset_text(wkt, wkt_length);
}

CassError dse_line_string_iterator_reset_with_wkt(DseLineStringIterator* iterator,
                                                  const char* wkt) {
  return dse_line_string_iterator_reset_with_wkt_n(iterator, wkt, SAFE_STRLEN(wkt));
}

cass_uint32_t dse_line_string_iterator_num_points(const DseLineStringIterator* iterator) {
  return iterator->num_points();
}

CassError dse_line_string_iterator_next_point(DseLineStringIterator* iterator,
                                              cass_double_t* x, cass_double_t* y) {
  return iterator->next_point(x, y);
}

} // extern "C"

namespace dse {

cass::String LineString::to_wkt() const {
  // Special case empty line string
  if (num_points_ == 0) {
    return "LINESTRING EMPTY";
  }

  cass::OStringStream ss;
  ss.precision(WKT_MAX_DIGITS);
  ss << "LINESTRING (";
  const cass_byte_t* pos = bytes_.data() + WKB_HEADER_SIZE + sizeof(cass_uint32_t);
  for (cass_uint32_t i = 0; i < num_points_; ++i) {
    if (i > 0) ss << ", ";
    ss << decode_double(pos, native_byte_order());
    pos += sizeof(cass_double_t);
    ss << " ";
    ss << decode_double(pos, native_byte_order());
    pos += sizeof(cass_double_t);
  }
  ss << ")";
  return ss.str();
}

CassError LineStringIterator::reset_binary(const CassValue* value) {
  size_t size;
  const cass_byte_t* pos;
  dse::WkbByteOrder byte_order;
  cass_uint32_t num_points;
  CassError rc;

  rc = dse::validate_data_type(value, DSE_LINE_STRING_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_LINE_STRING_HEADER_SIZE) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }
  size -= WKB_LINE_STRING_HEADER_SIZE;

  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_LINESTRING) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  num_points = dse::decode_uint32(pos, byte_order);
  pos += sizeof(cass_uint32_t);

  if (size < 2 * num_points * sizeof(cass_double_t)) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  num_points_ = num_points;
  binary_iterator_ = BinaryIterator(pos, pos + size, byte_order);
  iterator_ = &binary_iterator_;

  return CASS_OK;
}

bool isnum(int c) {
  return isdigit(c) || c == '+' || c == '-' || c == '.';
}

CassError LineStringIterator::reset_text(const char* text, size_t size) {
  cass_uint32_t num_points = 0;
  const bool skip_numbers = true;
  WktLexer lexer(text, size, skip_numbers);

  if (lexer.next_token() != WktLexer::TK_TYPE_LINESTRING) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  // Validate format and count the number of points
  WktLexer::Token token = lexer.next_token();

  // Special case "LINESTRING EMPTY"
  if (token == WktLexer::TK_EMPTY) {
    return CASS_OK;
  }

  if (token != WktLexer::TK_OPEN_PAREN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

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

    ++num_points;

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

  // Validate closing ")"
  if (token != WktLexer::TK_CLOSE_PAREN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  num_points_ = num_points;
  text_iterator_ = TextIterator(text, size);
  iterator_ = &text_iterator_;

  return CASS_OK;
}

CassError LineStringIterator::BinaryIterator::next_point(cass_double_t* x, cass_double_t* y) {
  if (position_ >= points_end_) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }

  *x = decode_double(position_, byte_order_);
  position_ += sizeof(cass_double_t);
  *y = decode_double(position_, byte_order_);
  position_ += sizeof(cass_double_t);

  return CASS_OK;
}

LineStringIterator::TextIterator::TextIterator(const char* text, size_t size)
  : lexer_(text, size) {
  WktLexer::Token token; UNUSED_(token);
  // Skip over "LINESTRING (" tokens
  token = lexer_.next_token(); assert(token == WktLexer::TK_TYPE_LINESTRING);
  token = lexer_.next_token(); assert(token == WktLexer::TK_OPEN_PAREN);
}

CassError LineStringIterator::TextIterator::next_point(cass_double_t* x, cass_double_t* y) {
  WktLexer::Token token;

  // If we're at the end of the text this will return WktLexer::TK_EOF
  token = lexer_.next_token();
  if (token != WktLexer::TK_NUMBER) {
    return CASS_ERROR_LIB_INVALID_STATE;
  }
  *x = lexer_.number();

  token = lexer_.next_token(); assert(token == WktLexer::TK_NUMBER);
  *y = lexer_.number();

  // Skip trailing "," or  ")" tokens
  token = lexer_.next_token(); assert(token == WktLexer::TK_COMMA ||
                                      token == WktLexer::TK_CLOSE_PAREN);

  return CASS_OK;
}

} // namespace dse
