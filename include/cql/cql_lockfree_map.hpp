#ifndef CQL_LOCKFREE_MAP_HPP_
#define CQL_LOCKFREE_MAP_HPP_

namespace cql {
	template<TKey, TValue>
	class cql_lockfree_map_t {
	public:
		// @expected_size - How many items you expect to
		// be in map
		cql_lockfree_map_t(size_t expected_size);
		
		// Tries to add specified key with specified value to map.
		// If key already exists in map returns false and DOESN'T change
		// value.
		bool try_insert(const TKey& key, const TValue& value);

		// Tries to get result from dictionary
		bool try_get(const TKey& key, TValue& result);
	};
}

#endif // CQL_LOCKFREE_MAP_HPP_