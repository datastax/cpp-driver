#ifndef CQL_QUERY_H_
#define CQL_QUERY_H_

#include "cql.hpp"
#include "cql/cql_r"

namespace cql {
	class cql_query_t {
	public:
	private:
		cql_consistency_enum	_consistency;
		bool					_trace_query;

	};
}

#endif // CQL_QUERY_H_