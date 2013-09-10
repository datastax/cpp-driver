#ifndef _CASSANDRA_SAFE_ADVANCE_H_INCLUDE_
#define _CASSANDRA_SAFE_ADVANCE_H_INCLUDE_

namespace Cassandra {
	// Advances iterator diff times (must be positive).
	// Stops when iterator reaches sequence end.
	template<typename TIterator>
	void safe_advance(TIterator& iterator, TIterator& end, size_t diff) {
		while(diff-- > 0) {
			if(iterator == end)
				break;
			++iterator;
		}
	}
}

#endif
