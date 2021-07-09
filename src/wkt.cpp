// clang-format off

#line 1 "wkt.rl"
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

#line 18 "wkt.cpp"
static const char _wkt_actions[] = { 0,  1, 0,  1, 1,  1, 2,  1, 5,  1, 6,  1, 7,  1,
                                     8,  1, 9,  1, 10, 1, 11, 1, 12, 1, 13, 1, 14, 1,
                                     15, 1, 16, 1, 17, 1, 18, 2, 2,  3, 2,  2, 4 };

static const char _wkt_key_offsets[] = {
  0,  2,  6,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
  21, 22, 23, 24, 25, 26, 27, 40, 43, 47, 49, 54, 56, 57, 58
};

static const char _wkt_trans_keys[] = { 48, 57, 43, 45,  48, 57, 48, 57, 80,  84, 89, 78,
                                        69, 83, 84, 82,  73, 78, 71, 73, 76,  78, 84, 89,
                                        71, 79, 78, 9,   32, 40, 41, 44, 46,  69, 76, 80,
                                        43, 45, 48, 57,  46, 48, 57, 69, 101, 48, 57, 48,
                                        57, 46, 69, 101, 48, 57, 48, 57, 77,  73, 79, 0 };

static const char _wkt_single_lengths[] = { 0, 2, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2,
                                            1, 1, 1, 1, 1, 1, 9, 1, 2, 0, 3, 0, 1, 1, 1 };

static const char _wkt_range_lengths[] = { 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 0, 0, 0 };

static const char _wkt_index_offsets[] = { 0,  2,  6,  8,  10, 12, 14, 16, 18, 20,
                                           22, 24, 26, 28, 30, 33, 35, 37, 39, 41,
                                           43, 45, 57, 60, 64, 66, 71, 73, 75, 77 };

static const char _wkt_trans_targs[] = {
  23, 21, 2,  2,  24, 21, 24, 21, 4,  21, 5,  21, 21, 21, 7,  21, 8,  21, 9,  21, 10, 21,
  11, 21, 12, 21, 13, 21, 21, 21, 15, 17, 21, 16, 21, 21, 21, 18, 21, 19, 21, 20, 21, 21,
  21, 21, 21, 21, 21, 21, 26, 27, 28, 29, 22, 25, 21, 0,  25, 21, 1,  1,  23, 21, 24, 21,
  0,  1,  1,  25, 21, 23, 21, 3,  21, 6,  21, 14, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21,
  21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 0
};

static const char _wkt_trans_actions[] = {
  5,  33, 0,  0,  0,  29, 0,  29, 0,  31, 0,  31, 13, 31, 0,  31, 0,  31, 0,  31, 0,  31,
  0,  31, 0,  31, 0,  31, 9,  31, 0,  0,  31, 0,  31, 7,  31, 0,  31, 0,  31, 0,  31, 11,
  31, 21, 21, 15, 17, 19, 0,  5,  5,  5,  38, 35, 23, 0,  35, 27, 0,  0,  5,  25, 0,  25,
  0,  0,  0,  35, 25, 5,  27, 0,  27, 0,  27, 0,  27, 33, 29, 29, 31, 31, 31, 31, 31, 31,
  31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 27, 25, 25, 25, 27, 27, 27, 27, 0
};

static const char _wkt_to_state_actions[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0 };

static const char _wkt_from_state_actions[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0 };

static const char _wkt_eof_trans[] = { 80,  82,  82,  100, 100, 100, 100, 100, 100, 100,
                                       100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                                       100, 0,   108, 104, 104, 104, 108, 108, 108, 108 };

static const int wkt_start = 21;
static const int wkt_first_final = 21;
static const int wkt_error = -1;

static const int wkt_en_main = 21;

#line 17 "wkt.rl"

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

#line 151 "wkt.cpp"
  {
    cs = wkt_start;
    ts = 0;
    te = 0;
    act = 0;
  }

#line 159 "wkt.cpp"
  {
    int _klen;
    unsigned int _trans;
    const char* _acts;
    unsigned int _nacts;
    const char* _keys;

    if (p == pe) goto _test_eof;
  _resume:
    _acts = _wkt_actions + _wkt_from_state_actions[cs];
    _nacts = (unsigned int)*_acts++;
    while (_nacts-- > 0) {
      switch (*_acts++) {
        case 1:
#line 1 "NONE"
        {
          ts = p;
        } break;
#line 178 "wkt.cpp"
      }
    }

    _keys = _wkt_trans_keys + _wkt_key_offsets[cs];
    _trans = _wkt_index_offsets[cs];

    _klen = _wkt_single_lengths[cs];
    if (_klen > 0) {
      const char* _lower = _keys;
      const char* _mid;
      const char* _upper = _keys + _klen - 1;
      while (1) {
        if (_upper < _lower) break;

        _mid = _lower + ((_upper - _lower) >> 1);
        if ((*p) < *_mid)
          _upper = _mid - 1;
        else if ((*p) > *_mid)
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
    if (_klen > 0) {
      const char* _lower = _keys;
      const char* _mid;
      const char* _upper = _keys + (_klen << 1) - 2;
      while (1) {
        if (_upper < _lower) break;

        _mid = _lower + (((_upper - _lower) >> 1) & ~1);
        if ((*p) < _mid[0])
          _upper = _mid - 2;
        else if ((*p) > _mid[1])
          _lower = _mid + 2;
        else {
          _trans += (unsigned int)((_mid - _keys) >> 1);
          goto _match;
        }
      }
      _trans += _klen;
    }

  _match:
  _eof_trans:
    cs = _wkt_trans_targs[_trans];

    if (_wkt_trans_actions[_trans] == 0) goto _again;

    _acts = _wkt_actions + _wkt_trans_actions[_trans];
    _nacts = (unsigned int)*_acts++;
    while (_nacts-- > 0) {
      switch (*_acts++) {
        case 2:
#line 1 "NONE"
        {
          te = p + 1;
        } break;
        case 3:
#line 49 "wkt.rl"
        {
          act = 8;
        } break;
        case 4:
#line 57 "wkt.rl"
        {
          act = 10;
        } break;
        case 5:
#line 42 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_TYPE_POINT;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 6:
#line 43 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_TYPE_LINESTRING;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 7:
#line 44 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_TYPE_POLYGON;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 8:
#line 45 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_EMPTY;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 9:
#line 46 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_OPEN_PAREN;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 10:
#line 47 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_CLOSE_PAREN;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 11:
#line 48 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_COMMA;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 12:
#line 56 "wkt.rl"
        {
          te = p + 1;
          { /* Skip */
          }
        } break;
        case 13:
#line 57 "wkt.rl"
        {
          te = p + 1;
          {
            token = TK_INVALID;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 14:
#line 49 "wkt.rl"
        {
          te = p;
          p--;
          {
            if (!skip_number_) {
              number_ = atof(datastax::String(ts, te).c_str());
            }
            token = TK_NUMBER;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 15:
#line 57 "wkt.rl"
        {
          te = p;
          p--;
          {
            token = TK_INVALID;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 16:
#line 49 "wkt.rl"
        {
          { p = ((te)) - 1; }
          {
            if (!skip_number_) {
              number_ = atof(datastax::String(ts, te).c_str());
            }
            token = TK_NUMBER;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 17:
#line 57 "wkt.rl"
        {
          { p = ((te)) - 1; }
          {
            token = TK_INVALID;
            {
              p++;
              goto _out;
            }
          }
        } break;
        case 18:
#line 1 "NONE"
        {
          switch (act) {
            case 8: {
              { p = ((te)) - 1; }
              if (!skip_number_) {
                number_ = atof(datastax::String(ts, te).c_str());
              }
              token = TK_NUMBER;
              {
                p++;
                goto _out;
              }
            } break;
            case 10: {
              { p = ((te)) - 1; }
              token = TK_INVALID;
              {
                p++;
                goto _out;
              }
            } break;
          }
        } break;
#line 337 "wkt.cpp"
      }
    }

  _again:
    _acts = _wkt_actions + _wkt_to_state_actions[cs];
    _nacts = (unsigned int)*_acts++;
    while (_nacts-- > 0) {
      switch (*_acts++) {
        case 0:
#line 1 "NONE"
        {
          ts = 0;
        } break;
#line 350 "wkt.cpp"
      }
    }

    if (++p != pe) goto _resume;
  _test_eof : {}
    if (p == eof) {
      if (_wkt_eof_trans[cs] > 0) {
        _trans = _wkt_eof_trans[cs] - 1;
        goto _eof_trans;
      }
    }

  _out : {}
  }

#line 62 "wkt.rl"

  position_ = p;

  return token;
}
