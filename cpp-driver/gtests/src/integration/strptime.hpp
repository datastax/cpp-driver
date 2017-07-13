#ifndef __STRPTIME_HPP__
#define __STRPTIME_HPP__

#include <time.h>

namespace test {

/**
 * strptime() is not available on all platforms. This implementation is from musl libc.
 *
 * @param s The string to be parsed
 * @param f The format of the string
 * @param tm The resulting time struct
 * @return NULL if an error occurred
 */
char *strptime(const char *s, const char *f, struct tm *tm);

} // namespace test

#endif // __STRPTIME_HPP__
