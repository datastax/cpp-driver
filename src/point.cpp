/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"
#include "wkt.hpp"

#include <string.h>

#include "macros.hpp"

extern "C" {

CassError dse_point_from_wkt(const char* wkt, cass_double_t* x, cass_double_t* y) {
  return dse_point_from_wkt_n(wkt, SAFE_STRLEN(wkt), x, y);
}

CassError dse_point_from_wkt_n(const char* wkt, size_t wkt_length, cass_double_t* x, cass_double_t* y) {
  WktLexer lexer(wkt, wkt_length);

  if (lexer.next_token() != WktLexer::TK_TYPE_POINT ||
      lexer.next_token() != WktLexer::TK_OPEN_PAREN ||
      lexer.next_token() != WktLexer::TK_NUMBER) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  *x = lexer.number();

  if (lexer.next_token() != WktLexer::TK_NUMBER) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  *y = lexer.number();

  // Look for the closing paren
  if (lexer.next_token() != WktLexer::TK_CLOSE_PAREN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  return CASS_OK;
}

} // extern "C"
