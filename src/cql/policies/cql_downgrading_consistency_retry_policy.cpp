/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */			
#include "cql/policies/cql_downgrading_consistency_retry_policy.hpp"
			
cql::cql_retry_decision_t
cql::cql_downgrading_consistency_retry_policy_t::read_timeout(				
	const cql_query_t& query,
	cql_consistency_enum consistency, 
	int required_responses, 
	int received_responses, 
	bool data_retrieved, 
	int retry_number
	)			
{				
	if( retry_number != 0 )	
		return cql_retry_decision_t::rethrow_decision();
				
	if (received_responses < required_responses)
	{		
		return max_likely_to_work_cl(received_responses);
	}		
			
	return !data_retrieved ? cql_retry_decision_t::retry_decision_with( consistency ) : cql_retry_decision_t::rethrow_decision();
}			
			
cql::cql_retry_decision_t
cql::cql_downgrading_consistency_retry_policy_t::write_timeout(
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
		
	if( write_type == "SIMPLE" || write_type == "BATCH" )
	{
		// Since we provide atomicity there is no point in retrying
		return cql_retry_decision_t::ignore();
	}
    else if( write_type == "COUNTER" )
	{
		// We should not retry counters, period!
		return cql_retry_decision_t::ignore();
	}
	else if( write_type == "UNLOGGED_BATCH" )
	{
		// Since only part of the batch could have been persisted,
		// retry with whatever consistency should allow to persist all
		return max_likely_to_work_cl( received_acks );
	}
	else if( write_type == "BATCH_LOG" )
	{
		return cql_retry_decision_t::retry_decision_with( consistency );
	}	
		
	return cql_retry_decision_t::rethrow_decision();
}		
				
		
cql::cql_retry_decision_t
cql::cql_downgrading_consistency_retry_policy_t::unavailable(
	const cql_query_t& query,
	cql_consistency_enum consistency, 
	int required_replica, 
	int alive_replica,
	int retry_number)
{			
	return retry_number != 0 ? cql_retry_decision_t::rethrow_decision() : max_likely_to_work_cl( alive_replica );
}			
		
		
cql::cql_retry_decision_t 
cql::cql_downgrading_consistency_retry_policy_t::max_likely_to_work_cl(int knownOk)
{		
	if (knownOk >= 3)
		return cql_retry_decision_t::retry_decision_with( cql_consistency_enum::CQL_CONSISTENCY_THREE );
	else if (knownOk >= 2)
		return cql_retry_decision_t::retry_decision_with( cql_consistency_enum::CQL_CONSISTENCY_TWO );
	else if (knownOk >= 1)
		return cql_retry_decision_t::retry_decision_with( cql_consistency_enum::CQL_CONSISTENCY_ONE );
	else
		return cql_retry_decision_t::rethrow_decision();
}		
		
		
