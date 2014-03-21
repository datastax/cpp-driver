#ifndef CQL_DEFAULT_RETRY_POLICY_H_
#define CQL_DEFAULT_RETRY_POLICY_H_

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#include "cql/policies/cql_retry_policy.hpp"

namespace cql {		
	class cql_downgrading_consistency_retry_policy_t:				
		public cql_retry_policy_t,
		boost::noncopyable
	{
	public:
		virtual cql_retry_decision_t
		read_timeout(
			const cql_query_t& query,
			cql_consistency_enum consistency,
			int required_responses,
			int received_responses,
			bool data_retrieved,
			int retry_number);

		virtual cql_retry_decision_t
		write_timeout(
			const cql_query_t& query,
			cql_consistency_enum consistency,
			const std::string& write_type,
			int required_acks,
			int received_acks,
			int retry_number
			);

		virtual cql_retry_decision_t
		unavailable(
			const cql_query_t& query,
			cql_consistency_enum consistency,
			int required_replica,
			int alive_replica,
			int retry_number);
						
		cql_downgrading_consistency_retry_policy_t() {};
						
	private:		
						
		cql::cql_retry_decision_t max_likely_to_work_cl( int knownOk );

	};
}

#endif
