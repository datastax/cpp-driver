#include "strptime.hpp"

#include <ctype.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <strings.h>
#endif

/*
  Implementation is from https://www.musl-libc.org/:

  Copyright © 2005-2014 Rich Felker, et al.

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

namespace test {

/* A restricted form of strptime() that doesn't support any locale based format options */
char* strptime(const char* s, const char* f, struct tm* v) {
  int i, w, neg, adj, min, range, *dest, dummy;
  int want_century = 0, century = 0, relyear = 0;
  while (*f) {
    if (*f != '%') {
      if (isspace(*f))
        for (; *s && isspace(*s); s++)
          ;
      else if (*s != *f)
        return 0;
      else
        s++;
      f++;
      continue;
    }
    f++;
    if (*f == '+') f++;
    if (isdigit(*f)) {
      char* new_f;
      w = strtoul(f, &new_f, 10);
      f = new_f;
    } else {
      w = -1;
    }
    adj = 0;
    switch (*f++) {
      case 'C':
        dest = &century;
        if (w < 0) w = 2;
        want_century |= 2;
        goto numeric_digits;
      case 'd':
      case 'e':
        dest = &v->tm_mday;
        min = 1;
        range = 31;
        goto numeric_range;
      case 'D':
        s = test::strptime(s, "%m/%d/%y", v);
        if (!s) return 0;
        break;
      case 'H':
        dest = &v->tm_hour;
        min = 0;
        range = 24;
        goto numeric_range;
      case 'I':
        dest = &v->tm_hour;
        min = 1;
        range = 12;
        goto numeric_range;
      case 'j':
        dest = &v->tm_yday;
        min = 1;
        range = 366;
        adj = 1;
        goto numeric_range;
      case 'm':
        dest = &v->tm_mon;
        min = 1;
        range = 12;
        adj = 1;
        goto numeric_range;
      case 'M':
        dest = &v->tm_min;
        min = 0;
        range = 60;
        goto numeric_range;
      case 'n':
      case 't':
        for (; *s && isspace(*s); s++)
          ;
        break;
      case 'R':
        s = test::strptime(s, "%H:%M", v);
        if (!s) return 0;
        break;
      case 'S':
        dest = &v->tm_sec;
        min = 0;
        range = 61;
        goto numeric_range;
      case 'T':
        s = test::strptime(s, "%H:%M:%S", v);
        if (!s) return 0;
        break;
      case 'U':
      case 'W':
        /* Throw away result, for now. (FIXME?) */
        dest = &dummy;
        min = 0;
        range = 54;
        goto numeric_range;
      case 'w':
        dest = &v->tm_wday;
        min = 0;
        range = 7;
        goto numeric_range;
      case 'y':
        dest = &relyear;
        w = 2;
        want_century |= 1;
        goto numeric_digits;
      case 'Y':
        dest = &v->tm_year;
        if (w < 0) w = 4;
        adj = 1900;
        want_century = 0;
        goto numeric_digits;
      case '%':
        if (*s++ != '%') return 0;
        break;
      default:
        return 0;
      numeric_range:
        if (!isdigit(*s)) return 0;
        *dest = 0;
        for (i = 1; i <= min + range && isdigit(*s); i *= 10)
          *dest = *dest * 10 + *s++ - '0';
        if ((unsigned)(*dest - min) >= (unsigned)range) return 0;
        *dest -= adj;
        switch ((char*)dest - (char*)v) { case offsetof(struct tm, tm_yday):; }
        goto update;
      numeric_digits:
        neg = 0;
        if (*s == '+')
          s++;
        else if (*s == '-')
          neg = 1, s++;
        if (!isdigit(*s)) return 0;
        for (*dest = i = 0; i < w && isdigit(*s); i++)
          *dest = *dest * 10 + *s++ - '0';
        if (neg) *dest = -*dest;
        *dest -= adj;
        goto update;
      update
          :
          // FIXME
          ;
    }
  }
  if (want_century) {
    v->tm_year = relyear;
    if (want_century & 2)
      v->tm_year += century * 100 - 1900;
    else if (v->tm_year <= 68)
      v->tm_year += 100;
  }
  return (char*)s;
}

} // namespace test
