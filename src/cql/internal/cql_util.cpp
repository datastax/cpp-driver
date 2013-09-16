
#include <cassert>
#include <cstring>

#include "cql/internal/cql_util.hpp"

char* 
cql::safe_strncpy(char* dest, const char* src, const size_t count) {
	assert(count > 0);
	
	strncpy(dest, src, count - 1);
	// null terminate destination
	dest[count - 1] = '\0';

	return dest;
}