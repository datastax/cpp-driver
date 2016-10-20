
#line 1 "wkt.rl"
/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "wkt.hpp"

#include <stdlib.h>
#include <string>


#line 17 "wkt.cpp"
static const char _wkt_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	5, 1, 6, 1, 7, 1, 8, 1, 
	9, 1, 10, 1, 11, 1, 12, 1, 
	13, 1, 14, 1, 15, 1, 16, 1, 
	17, 2, 2, 3, 2, 2, 4
};

static const char _wkt_key_offsets[] = {
	0, 2, 6, 8, 9, 10, 11, 12, 
	13, 14, 15, 16, 18, 19, 20, 21, 
	22, 23, 24, 36, 39, 43, 45, 50, 
	52, 53
};

static const char _wkt_trans_keys[] = {
	48, 57, 43, 45, 48, 57, 48, 57, 
	78, 69, 83, 84, 82, 73, 78, 71, 
	73, 76, 78, 84, 89, 71, 79, 78, 
	9, 32, 40, 41, 44, 46, 76, 80, 
	43, 45, 48, 57, 46, 48, 57, 69, 
	101, 48, 57, 48, 57, 46, 69, 101, 
	48, 57, 48, 57, 73, 79, 0
};

static const char _wkt_single_lengths[] = {
	0, 2, 0, 1, 1, 1, 1, 1, 
	1, 1, 1, 2, 1, 1, 1, 1, 
	1, 1, 8, 1, 2, 0, 3, 0, 
	1, 1
};

static const char _wkt_range_lengths[] = {
	1, 1, 1, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 2, 1, 1, 1, 1, 1, 
	0, 0
};

static const char _wkt_index_offsets[] = {
	0, 2, 6, 8, 10, 12, 14, 16, 
	18, 20, 22, 24, 27, 29, 31, 33, 
	35, 37, 39, 50, 53, 57, 59, 64, 
	66, 68
};

static const char _wkt_trans_targs[] = {
	20, 18, 2, 2, 21, 18, 21, 18, 
	4, 18, 5, 18, 6, 18, 7, 18, 
	8, 18, 9, 18, 10, 18, 18, 18, 
	12, 14, 18, 13, 18, 18, 18, 15, 
	18, 16, 18, 17, 18, 18, 18, 18, 
	18, 18, 18, 18, 23, 24, 25, 19, 
	22, 18, 0, 22, 18, 1, 1, 20, 
	18, 21, 18, 0, 1, 1, 22, 18, 
	20, 18, 3, 18, 11, 18, 18, 18, 
	18, 18, 18, 18, 18, 18, 18, 18, 
	18, 18, 18, 18, 18, 18, 18, 18, 
	18, 18, 18, 18, 18, 18, 18, 0
};

static const char _wkt_trans_actions[] = {
	5, 31, 0, 0, 0, 27, 0, 27, 
	0, 29, 0, 29, 0, 29, 0, 29, 
	0, 29, 0, 29, 0, 29, 9, 29, 
	0, 0, 29, 0, 29, 7, 29, 0, 
	29, 0, 29, 0, 29, 11, 29, 19, 
	19, 13, 15, 17, 0, 5, 5, 36, 
	33, 21, 0, 33, 25, 0, 0, 5, 
	23, 0, 23, 0, 0, 0, 33, 23, 
	5, 25, 0, 25, 0, 25, 31, 27, 
	27, 29, 29, 29, 29, 29, 29, 29, 
	29, 29, 29, 29, 29, 29, 29, 29, 
	25, 23, 23, 23, 25, 25, 25, 0
};

static const char _wkt_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 1, 0, 0, 0, 0, 0, 
	0, 0
};

static const char _wkt_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 3, 0, 0, 0, 0, 0, 
	0, 0
};

static const char _wkt_eof_trans[] = {
	71, 73, 73, 88, 88, 88, 88, 88, 
	88, 88, 88, 88, 88, 88, 88, 88, 
	88, 88, 0, 95, 92, 92, 92, 95, 
	95, 95
};

static const int wkt_start = 18;
static const int wkt_first_final = 18;
static const int wkt_error = -1;

static const int wkt_en_main = 18;


#line 16 "wkt.rl"



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

  
#line 144 "wkt.cpp"
	{
	cs = wkt_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 152 "wkt.cpp"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
_resume:
	_acts = _wkt_actions + _wkt_from_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 1:
#line 1 "NONE"
	{ts = p;}
	break;
#line 171 "wkt.cpp"
		}
	}

	_keys = _wkt_trans_keys + _wkt_key_offsets[cs];
	_trans = _wkt_index_offsets[cs];

	_klen = _wkt_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _wkt_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
_eof_trans:
	cs = _wkt_trans_targs[_trans];

	if ( _wkt_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _wkt_actions + _wkt_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 2:
#line 1 "NONE"
	{te = p+1;}
	break;
	case 3:
#line 47 "wkt.rl"
	{act = 7;}
	break;
	case 4:
#line 55 "wkt.rl"
	{act = 9;}
	break;
	case 5:
#line 41 "wkt.rl"
	{te = p+1;{ token = TK_TYPE_POINT; {p++; goto _out; } }}
	break;
	case 6:
#line 42 "wkt.rl"
	{te = p+1;{ token = TK_TYPE_LINESTRING; {p++; goto _out; } }}
	break;
	case 7:
#line 43 "wkt.rl"
	{te = p+1;{ token = TK_TYPE_POLYGON; {p++; goto _out; } }}
	break;
	case 8:
#line 44 "wkt.rl"
	{te = p+1;{ token = TK_OPEN_PAREN; {p++; goto _out; } }}
	break;
	case 9:
#line 45 "wkt.rl"
	{te = p+1;{ token = TK_CLOSE_PAREN; {p++; goto _out; } }}
	break;
	case 10:
#line 46 "wkt.rl"
	{te = p+1;{ token = TK_COMMA; {p++; goto _out; } }}
	break;
	case 11:
#line 54 "wkt.rl"
	{te = p+1;{ /* Skip */ }}
	break;
	case 12:
#line 55 "wkt.rl"
	{te = p+1;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 13:
#line 47 "wkt.rl"
	{te = p;p--;{
                   if (!skip_number_) {
                     number_ = atof(std::string(ts, te).c_str());
                   }
                   token = TK_NUMBER;
                   {p++; goto _out; }
                }}
	break;
	case 14:
#line 55 "wkt.rl"
	{te = p;p--;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 15:
#line 47 "wkt.rl"
	{{p = ((te))-1;}{
                   if (!skip_number_) {
                     number_ = atof(std::string(ts, te).c_str());
                   }
                   token = TK_NUMBER;
                   {p++; goto _out; }
                }}
	break;
	case 16:
#line 55 "wkt.rl"
	{{p = ((te))-1;}{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 17:
#line 1 "NONE"
	{	switch( act ) {
	case 7:
	{{p = ((te))-1;}
                   if (!skip_number_) {
                     number_ = atof(std::string(ts, te).c_str());
                   }
                   token = TK_NUMBER;
                   {p++; goto _out; }
                }
	break;
	case 9:
	{{p = ((te))-1;} token = TK_INVALID; {p++; goto _out; } }
	break;
	}
	}
	break;
#line 326 "wkt.cpp"
		}
	}

_again:
	_acts = _wkt_actions + _wkt_to_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 0:
#line 1 "NONE"
	{ts = 0;}
	break;
#line 339 "wkt.cpp"
		}
	}

	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	if ( _wkt_eof_trans[cs] > 0 ) {
		_trans = _wkt_eof_trans[cs] - 1;
		goto _eof_trans;
	}
	}

	_out: {}
	}

#line 60 "wkt.rl"


  position_ = p;

  return token;
}
