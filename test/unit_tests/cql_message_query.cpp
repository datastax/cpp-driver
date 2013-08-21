#include <boost/test/unit_test.hpp>
#include "libcql/cql.hpp"
#include "libcql/cql_error.hpp"
#include "libcql/internal/cql_defines.hpp"
#include "libcql/internal/cql_message_query_impl.hpp"

BOOST_AUTO_TEST_SUITE(cql_message_query)

char TEST_MESSAGE_QUERY[] = {
    0x00, 0x00, 0x00, 0x0b, 0x75, 0x73, 0x65, 0x20,
    0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x3b, 0x00,
    0x05 };

BOOST_AUTO_TEST_CASE(opcode)
{
	cql::cql_message_query_impl_t m;
	BOOST_CHECK_EQUAL(cql::CQL_OPCODE_QUERY, m.opcode());
}

BOOST_AUTO_TEST_CASE(serialization_to_byte)
{
	cql::cql_message_query_impl_t m("use system;", cql::CQL_CONSISTENCY_ALL);
    cql::cql_error_t err;
    m.prepare(&err);
    BOOST_CHECK_EQUAL(sizeof(TEST_MESSAGE_QUERY), m.size());
    BOOST_CHECK(memcmp(TEST_MESSAGE_QUERY, &(*m.buffer())[0], sizeof(TEST_MESSAGE_QUERY)) == 0);
}

BOOST_AUTO_TEST_CASE(serialization_from_byte)
{
	cql::cql_message_query_impl_t m;
    m.buffer()->assign(TEST_MESSAGE_QUERY, TEST_MESSAGE_QUERY + sizeof(TEST_MESSAGE_QUERY));
    cql::cql_error_t err;
    m.consume(&err);

	BOOST_CHECK_EQUAL("use system;", m.query());
	BOOST_CHECK_EQUAL(cql::CQL_CONSISTENCY_ALL, m.consistency());
}

BOOST_AUTO_TEST_SUITE_END()
