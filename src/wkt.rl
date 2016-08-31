/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "wkt.hpp"

#include <stdlib.h>
#include <string>

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
      '(' => { token = TK_OPEN_PAREN; fbreak; };
      ')' => { token = TK_CLOSE_PAREN; fbreak; };
      ',' => { token = TK_COMMA; fbreak; };
      number => {
                   if (!skip_number_) {
                     number_ = atof(std::string(ts, te).c_str());
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
