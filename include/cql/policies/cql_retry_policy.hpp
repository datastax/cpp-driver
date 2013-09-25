#ifndef CQL_RETRY_POLICY_H_
#define CQL_RETRY_POLICY_H_

#include <string>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>
#include "cql/cql.hpp"

namespace cql {
	enum cql_retry_decision_enum {
		CQL_RETRY_DECISION_RETRY,
		CQL_RETRY_DECISION_RETHROW,
		CQL_RETRY_DECISION_IGNORE
	};

	class cql_retry_decision_t {
	public:
		inline cql_retry_decision_enum
		retry_decision() const { return _retry_decision; }

		inline const boost::optional<cql_consistency_enum>&
		consistency_level() const { return _consistency; }

		inline static cql_retry_decision_t 
		rethrow_decision() {
			return cql_retry_decision_t(
				CQL_RETRY_DECISION_RETHROW, 
				CQL_CONSISTENCY_DEFAULT);
		}

		inline static cql_retry_decision_t
		retry_decision_with(const boost::optional<cql_consistency_enum>& consistency) {
			return cql_retry_decision_t(
				CQL_RETRY_DECISION_RETRY,
				consistency);
		}

		inline static cql_retry_decision_t
		ignore() {
			return cql_retry_decision_t(
				CQL_RETRY_DECISION_IGNORE,
				CQL_CONSISTENCY_DEFAULT);
		}

	private:
		cql_retry_decision_t(
			cql_retry_decision_enum retry_decision,
			const boost::optional<cql_consistency_enum>& consistency)
			: _retry_decision(retry_decision),
			_consistency(_consistency) { }

		cql_retry_decision_enum					_retry_decision;
		boost::optional<cql_consistency_enum>	_consistency;
	};

	class cql_query_t;

	class cql_retry_policy_t {
	public:
		virtual cql_retry_decision_t
		read_timeout(
			const cql_query_t& query,
			cql_consistency_enum consistency, 
			int required_responses, 
			int received_responses, 
			bool data_retrieved, 
			int retry_number) = 0;

		virtual cql_retry_decision_t
		write_timeout(
			const cql_query_t& query,
			cql_consistency_enum consistency, 
			const std::string& write_type,
			int required_acks, 
			int received_acks, 
			int retry_number
			) = 0;

		virtual cql_retry_decision_t
		unavailable(
			const cql_query_t& query,
			cql_consistency_enum consistency, 
			int required_replica, 
			int alive_replica,
			int retry_number) = 0;

		virtual ~cql_retry_policy_t() { }
	};
}

#endif // CQL_CQL_QUERY_H_