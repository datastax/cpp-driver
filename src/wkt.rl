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

#include "wkt.hpp"

#include "string.hpp"

#include <stdlib.h>

%%{
  machine wkt;
  write data;
}%%


WktLexer::Token WktLexer::next_token() {
  Token token = TK_INVALID;

  const char* p = position_;
  const char* pe = end_;
  const char* ts;
  const char* te;
  const char* eof = pe;
  int cs, act;

  // Avoid unused variable warning
  (void)wkt_first_final;
  (void)wkt_error;
  (void)wkt_en_main;

  if (p == eof) return TK_EOF;

  %%{
    ws = [ \t];
    number = [+\-]?  [0-9]* '.'? [0-9]+ ([eE] [+\-]? [0-9]+)?;

    main := |*
      'POINT' => { token = TK_TYPE_POINT; fbreak; };
      'LINESTRING' => { token = TK_TYPE_LINESTRING; fbreak; };
      'POLYGON' => { token = TK_TYPE_POLYGON; fbreak; };
      'EMPTY' => { token = TK_EMPTY; fbreak; };
      '(' => { token = TK_OPEN_PAREN; fbreak; };
      ')' => { token = TK_CLOSE_PAREN; fbreak; };
      ',' => { token = TK_COMMA; fbreak; };
      number => {
                   if (!skip_number_) {
                     number_ = atof(datastax::String(ts, te).c_str());
                   }
                   token = TK_NUMBER;
                   fbreak;
                };
      ws => { /* Skip */ };
      any => { token = TK_INVALID; fbreak; };
    *|;

    write init;
    write exec;
  }%%

  position_ = p;

  return token;
}
