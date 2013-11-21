#include <boost/test/unit_test.hpp>
#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"

BOOST_AUTO_TEST_SUITE(simple_test1)

BOOST_AUTO_TEST_CASE(test1)
{
	int numberOfNodes = 1;

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
		
	BOOST_CHECK_EQUAL(1, 1);
}

BOOST_AUTO_TEST_SUITE_END()
