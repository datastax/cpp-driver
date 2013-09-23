#include "cql/policies/cql_default_retry_policy.hpp"

boost::shared_ptr<cql_default_retry_policy_t> 
get_default_retry_policy() {
	return boost::shared_ptr<cql_default_policy_t>(new cql_default_policy_t());
}

cql_retry_decision_t
cql::cql_default_retry_policy_t::read_timeout(
	const cql_query_t& query,
	cql_consistency_enum consistency, 
	int required_responses, 
	int received_responses, 
	bool data_retrieved, 
	int retry_number
	) 
{
	if (retry_number != 0)
		return cql_retry_decision_t::rethrow_decision();

	return (received_responses >= required_responses) && !data_detrieved
		? cql_retry_decision_t::retry_decision_with(consistency)
		: cql_retry_decision_t::rethrow_decision();
}

cql_retry_decision_t
cql::cql_default_retry_policy_t::write_timeout(
	const cql_query_t& query,
	cql_consistency_enum consistency, 
	const std::string& write_type,
	int required_acks, 
	int received_acks, 
	int retry_number
	)
{
	if (retry_number != 0)
		return cql_retry_decision_t::rethrow_decision();

	return (write_type == "BATCH_LOG") 
		? cql_retry_decision_t::retry_decision_with(consistency)
		: cql_retry_decision_t::rethrow_decision();
}

cql_retry_decision_t
cql::cql_default_retry_policy_t::unavailable(
	const cql_query_t& query,
	cql_consistency_enum consistency, 
	int required_replica, 
	int alive_replica,
	int retry_number)
{
	return cql_retry_decision_t::rethrow_decision();
}