#ifndef CQL_ROUTING_KEY_H_
#define CQL_ROUTING_KEY_H_

#include <vector>
#include <boost/shared_ptr.hpp>

#include "cql/cql.hpp"

namespace cql {
	class cql_routing_key_t {
	public:
		typedef
			std::vector<cql_byte_t>
			bytes_t;

		typedef
			bytes_t::iterator
			iterator;

		typedef
			bytes_t::const_iterator
			const_iterator;

		static boost::shared_ptr<cql_routing_key_t> 
		empty();

		inline const_iterator
		key_bytes_cbegin() const { return _bytes.cbegin(); }

		inline const_iterator
		key_bytes_cend() const { return _bytes.cend(); }

		inline iterator
		key_bytes_begin() { return _bytes.begin(); }

		inline iterator
		key_bytes_end() { return _bytes.end(); }

		template<TIterator>
		inline void
		set_key_bytes(TIterator begin, TIterator end) {
			_bytes.assign(begin, end);
		}

	private:
		std::vector<cql_byte_t> _bytes;
	};
}

#endif // CQL_ROUTING_KEY_H_