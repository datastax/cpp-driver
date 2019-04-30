/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_ENTERPRISE_INTERNAL_WKT_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_WKT_HPP

#include <stddef.h>

#define WKT_MAX_DIGITS 17

#define WKT_TOKEN_MAP(XX) \
  XX(TK_INVALID) \
  XX(TK_EOF) \
  XX(TK_TYPE_POINT) \
  XX(TK_TYPE_LINESTRING) \
  XX(TK_TYPE_POLYGON) \
  XX(TK_NUMBER) \
  XX(TK_COMMA) \
  XX(TK_EMPTY) \
  XX(TK_OPEN_PAREN) \
  XX(TK_CLOSE_PAREN)

class WktLexer {
public:
  enum Token {
#define XX_TOKEN(token) token,
    WKT_TOKEN_MAP(XX_TOKEN)
#undef XX_TOKEN
    TK_LAST_ENTRY
  };

  WktLexer() { }
  WktLexer(const char* text, size_t size, bool skip_number = false)
    : position_(text)
    , end_(text + size)
    , skip_number_(skip_number) { }
  WktLexer(const WktLexer& other, bool skip_number = false)
    : position_(other.position_)
    , end_(other.end_)
    , skip_number_(skip_number) { }

  double number() const { return number_; }

  Token next_token();

  static const char* to_string(Token token) {
    switch (token) {
#define XX_TOKEN(token) case token: return #token;
    WKT_TOKEN_MAP(XX_TOKEN)
#undef XX_TOKEN
      default: return "";
    }
  }

private:
  double number_;
  const char* position_;
  const char* end_;
  bool skip_number_;
};

#endif
