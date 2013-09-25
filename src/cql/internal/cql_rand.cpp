#include <cstdlib>
#include <boost/atomic.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

#include "cql/internal/cql_rand.hpp"

int
cql::cql_rand() {
	static boost::detail::spinlock sl;
	static bool rand_initialized = false;

	boost::lock_guard<boost::detail::spinlock> lock(sl);
	if(!rand_initialized)
		srand((unsigned)time(NULL));

	return rand();
}