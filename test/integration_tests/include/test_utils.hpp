#pragma once
#include "cql/cql.hpp"

#include <boost/asio/ip/address.hpp>

// Forward declarations
namespace cql {
    class cql_result_t;
    class cql_session_t;
    class cql_ccm_bridge_t;
    class cql_builder_t;
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
    
	std::string
    get_cql(cql::cql_column_type_enum);

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
    CCM_SETUP(int numberOfNodes);
    virtual ~CCM_SETUP();
    
	boost::shared_ptr<cql::cql_ccm_bridge_t>   ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address                   ccm_contact_seed;
	boost::shared_ptr<cql::cql_builder_t>      builder;
    
	bool use_ssl;
};

} // End of namespace test_utils
