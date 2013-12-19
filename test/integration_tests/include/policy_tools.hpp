#pragma once
#include "cql/cql.hpp"

namespace policy_tools{

	extern std::map<boost::asio::ip::address, int> coordinators;
	
	void 
	create_schema(
		boost::shared_ptr<cql::cql_session_t> session,
		int replicationFactor);

	int 
	init(
		boost::shared_ptr<cql::cql_session_t> session,
		int n,
		cql::cql_consistency_enum cl,
		bool batch = false);

	void 
	add_coordinator(boost::asio::ip::address coord_addr);

	void
	reset_coordinators();

	void 
	assertQueried(
		boost::asio::ip::address coord_addr,
		int n);

	void 
	assertQueriedAtLeast(
		boost::asio::ip::address coord_addr, 
		int n);

	int
	query(
		boost::shared_ptr<cql::cql_session_t> session,
		int n,
		cql::cql_consistency_enum cl);
	
} // End of namespace policy_tools