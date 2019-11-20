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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_WKT_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_WKT_HPP

#include <stddef.h>

#define WKT_MAX_DIGITS 17

#define WKT_TOKEN_MAP(XX) \
  XX(TK_INVALID)          \
  XX(TK_EOF)              \
  XX(TK_TYPE_POINT)       \
  XX(TK_TYPE_LINESTRING)  \
  XX(TK_TYPE_POLYGON)     \
  XX(TK_NUMBER)           \
  XX(TK_COMMA)            \
  XX(TK_EMPTY)            \
  XX(TK_OPEN_PAREN)       \
  XX(TK_CLOSE_PAREN)

class WktLexer {
public:
  enum Token {
#define XX_TOKEN(token) token,
    WKT_TOKEN_MAP(XX_TOKEN)
#undef XX_TOKEN
        TK_LAST_ENTRY
  };

  WktLexer() {}
  WktLexer(const char* text, size_t size, bool skip_number = false)
      : position_(text)
      , end_(text + size)
      , skip_number_(skip_number) {}
  WktLexer(const WktLexer& other, bool skip_number = false)
      : position_(other.position_)
      , end_(other.end_)
      , skip_number_(skip_number) {}

  WktLexer& operator=(const WktLexer& other) {
    position_ = other.position_;
    end_ = other.end_;
    skip_number_ = other.skip_number_;
    return *this;
  }

  double number() const { return number_; }

  Token next_token();

  static const char* to_string(Token token) {
    switch (token) {
#define XX_TOKEN(token) \
  case token:           \
    return #token;
      WKT_TOKEN_MAP(XX_TOKEN)
#undef XX_TOKEN
      default:
        return "";
    }
  }

private:
  double number_;
  const char* position_;
  const char* end_;
  bool skip_number_;
};

#endif
