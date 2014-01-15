#pragma once
#include "cql/cql.hpp"
#include "cql/cql_session.hpp"
#include "cql/cql_execute.hpp"
#include "cql/cql_result.hpp"

#include <boost/asio/ip/address.hpp>

// Forward declarations
namespace cql {
    class cql_result_t;
    class cql_session_t;
	class cql_cluster_t;
    class cql_ccm_bridge_t;
    class cql_builder_t;
	class cql_execute_t;
}

/** Random, reusable tools for testing. */
namespace test_utils {
    
	void
    log_callback(
        const cql::cql_short_t,
        const std::string& message);
    
	boost::shared_ptr<cql::cql_result_t>
    query(
        boost::shared_ptr<cql::cql_session_t>,
        std::string,
        cql::cql_consistency_enum cl = cql::CQL_CONSISTENCY_ONE);
    
	template<class T>
	boost::shared_ptr<cql::cql_result_t>
	prepared_query(
		boost::shared_ptr<cql::cql_session_t> session,
		std::string query_string,
		T binding_value,
		cql::cql_consistency_enum cl = cql::CQL_CONSISTENCY_ONE);
	
	template<class T>
	boost::shared_ptr<cql::cql_result_t>
	prepared_query(
		boost::shared_ptr<cql::cql_session_t> session,
		std::string query_string,
		T binding_value,
		cql::cql_consistency_enum cl)
	{
		boost::shared_ptr<cql::cql_query_t> _prep_query(
			new cql::cql_query_t(query_string,cl));
		boost::shared_future<cql::cql_future_result_t> query_future = session->prepare(_prep_query);
		query_future.wait();
		assert(!query_future.get().error.is_err());

		std::vector<cql::cql_byte_t> queryid = query_future.get().result->query_id();
		boost::shared_ptr<cql::cql_execute_t> bound(
			new cql::cql_execute_t(queryid, cl));

		bound->push_back(binding_value);

		query_future = session->execute(bound);
		query_future.wait();

		cql::cql_future_result_t query_result = query_future.get();
		return query_result.result;
	}

	std::string
    get_cql(cql::cql_column_type_enum);

	void
		waitFor(
		boost::asio::ip::address node,
		boost::shared_ptr<cql::cql_cluster_t> cluster,
		int maxTry,
		bool waitForDead,
		bool waitForOut);

	void 
	waitForDownWithWait(
		boost::asio::ip::address node,
		boost::shared_ptr<cql::cql_cluster_t> cluster,
		int waitTime);


	extern const std::string CREATE_KEYSPACE_SIMPLE_FORMAT;
	extern const std::string CREATE_KEYSPACE_GENERIC_FORMAT;
	extern const std::string SIMPLE_KEYSPACE;
	extern const std::string SIMPLE_TABLE;
	extern const std::string CREATE_TABLE_SIMPLE_FORMAT;
	extern const std::string INSERT_FORMAT;
	extern const std::string SELECT_ALL_FORMAT;
	extern const std::string SELECT_WHERE_FORMAT;

/** The following class cannot be used as a kernel of test fixture because of
    parametrized ctor. Derive from it to use it in your tests.
 */
struct CCM_SETUP {
    CCM_SETUP(int numberOfNodesDC1, int numberOfNodesDC2);
    virtual ~CCM_SETUP();
    
	boost::shared_ptr<cql::cql_ccm_bridge_t>   ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address                   ccm_contact_seed;
	boost::shared_ptr<cql::cql_builder_t>      builder;
    
	bool use_ssl;
};

} // End of namespace test_utils
