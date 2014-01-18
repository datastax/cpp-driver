#include "cql/cql.hpp"
#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql_ccm_bridge.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

#include "test_utils.hpp"

namespace policy_tools{

std::map<boost::asio::ip::address, int> coordinators;

void 
create_schema(
	boost::shared_ptr<cql::cql_session_t> session,
	int replicationFactor)
{		
	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % replicationFactor));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);
	test_utils::query(session, str(boost::format("CREATE TABLE %s (k int PRIMARY KEY, i int)") % test_utils::SIMPLE_TABLE));
}

int 
init(
	boost::shared_ptr<cql::cql_session_t> session,
	int n,
	cql::cql_consistency_enum cl,
	bool batch)
{
	std::string query_string = str(boost::format("INSERT INTO %s(k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE);	

	if(batch)
	{
		std::string bth;
		bth.append("BEGIN BATCH ");
		bth.append(str(boost::format("INSERT INTO %s(k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE));
		bth.append(" APPLY BATCH");
		query_string = bth;
	}

	for (int i = 0; i < n; ++i)
	{
		boost::shared_ptr<cql::cql_query_t> _query(
			new cql::cql_query_t(query_string ,cl));
		boost::shared_future<cql::cql_future_result_t> query_future = session->query(_query);
		query_future.wait();

		cql::cql_future_result_t query_result = query_future.get();

		if(query_result.error.code != 0)
			return query_result.error.code;		
	}
	return 0;
}

void 
add_coordinator(
	boost::asio::ip::address coord_addr)
{
	if(coordinators.count(coord_addr) == 0)
		coordinators.insert(std::make_pair(coord_addr, 1));
	else
		coordinators[coord_addr] += 1;
}

void 
reset_coordinators()
{
	coordinators.clear();	
}

void 
assertQueried(
	boost::asio::ip::address coord_addr,
	int n)
{
	if(coordinators.count(coord_addr)!=0)	 
		BOOST_REQUIRE(coordinators[coord_addr] == n);
	else
		BOOST_REQUIRE(n == 0);
}

void 
assertQueriedAtLeast(
	boost::asio::ip::address coord_addr,
	int n)
{
	int queried = coordinators[coord_addr];
	BOOST_REQUIRE(queried >= n);
}

int
query(
	boost::shared_ptr<cql::cql_session_t> session,
	int n,
	cql::cql_consistency_enum cl)
{
	for (int i = 0; i < n; ++i)
	{
		boost::shared_ptr<cql::cql_query_t> _query(
			new cql::cql_query_t(str(boost::format("SELECT * FROM %s WHERE k = 0") % test_utils::SIMPLE_TABLE),cl));
		boost::shared_future<cql::cql_future_result_t> query_future = session->query(_query);
		if (!(query_future.timed_wait(boost::posix_time::seconds(10)))) {
            BOOST_FAIL("Query timed out");
        }

		cql::cql_future_result_t query_result = query_future.get();
		std::cout << "Querying endpoint: " << query_result.client->endpoint().to_string() << std::endl;

		add_coordinator(query_result.client->endpoint().address());		
		if(query_result.error.code != 0)
			return query_result.error.code;							
	}
	return 0;
}

} // End of namespace policy_tools
