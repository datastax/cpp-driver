#include <boost/test/unit_test.hpp>
#include "cassandra/cql.hpp"
#include "cassandra/cql_error.hpp"
#include "cassandra/internal/cql_defines.hpp"
#include "cassandra/internal/cql_message_ready_impl.hpp"

BOOST_AUTO_TEST_SUITE(cql_message_ready)

BOOST_AUTO_TEST_CASE(opcode)
{
	cql::cql_message_ready_impl_t m;
	BOOST_CHECK_EQUAL(cql::CQL_OPCODE_READY, m.opcode());
}

BOOST_AUTO_TEST_CASE(zero_length_serialization)
{
	cql::cql_message_ready_impl_t m;
    cql::cql_error_t err;
    m.prepare(&err);
    BOOST_CHECK_EQUAL(0, m.size());
}

BOOST_AUTO_TEST_SUITE_END()
