#include <cstdlib>
#include <boost/atomic.hpp>

#include "cql/cql_rand.hpp"

int
cql::cql_rand() {
	static boost::detail::spinlock sl;
	static bool rand_initialized = false;

	boost::lock_guard<boost::detail::spinlock> lock(sl);
	if(!rand_initialized)
		srand((unsinged)time(NULL));

	return rand();
}