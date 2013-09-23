#ifndef CQL_QUERY_H_
#define CQL_QUERY_H_

#include "cql.hpp"
#include "cql/policies/cql_retry_policy.hpp"
#include "cql/cql_routing_key.hpp"

namespace cql {
	class cql_query_t {
	public:

		inline bool 
		tracing_enabled() const { return _trace_query; }

		inline void
		enable_tracing() { _trace_query = true; }

		inline void
		disable_tracing() { _trace_query = false; }

		inline cql_consistency_enum
		consistency() const { return _consistency; }

		inline void
		set_consistency(const cql_consistency_enum& consistency) {
			_consistency = consistency;
		}

		inline boost::shared_ptr<cql_retry_policy_t>
		retry_policy() { return _retry_policy; }

		inline void
		set_retry_policy(const boost::shared_ptr<cql_retry_policy_t>& retry_policy) {
			_retry_policy = retry_policy;
		}

		cql_query_t()
			: _consistency(CQL_CONSISTENCY_DEFAULT),
			  _trace_query(false),
			  _retry_policy(NULL) { }

		virtual ~cql_query_t() { }

		virtual boost::shared_ptr<cql_routing_key_t>
		get_routing_key() = 0;



	private:
		cql_consistency_enum					_consistency;
		bool									_trace_query;
		boost::shared_ptr<cql_retry_policy_t>	_retry_policy;
	};
}

#endif // CQL_QUERY_H_