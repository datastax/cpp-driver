#include <boost/test/unit_test.hpp>
#include "libcql/cql.hpp"
#include "libcql/cql_error.hpp"
#include "libcql/internal/cql_defines.hpp"
#include "libcql/internal/cql_message_startup_impl.hpp"

BOOST_AUTO_TEST_SUITE(cql_message_startup)

char TEST_MESSAGE_STARTUP[] = {
    0x00, 0x01, 0x00, 0x0b, 0x43, 0x51, 0x4c, 0x5f,
    0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e, 0x00,
    0x05, 0x33, 0x2e, 0x30, 0x2e, 0x30 };

BOOST_AUTO_TEST_CASE(opcode)
{
	cql::cql_message_startup_impl_t m;
	BOOST_CHECK_EQUAL(cql::CQL_OPCODE_STARTUP, m.opcode());
}

BOOST_AUTO_TEST_CASE(serialization_to_byte)
{
	cql::cql_message_startup_impl_t m;
    m.version(CQL_VERSION_IMPL);
    cql::cql_error_t err;
    m.prepare(&err);
    BOOST_CHECK_EQUAL(sizeof(TEST_MESSAGE_STARTUP), m.size());
    BOOST_CHECK(memcmp(TEST_MESSAGE_STARTUP, &(*m.buffer())[0], sizeof(TEST_MESSAGE_STARTUP)) == 0);
}

BOOST_AUTO_TEST_CASE(serialization_from_byte)
{
	cql::cql_message_startup_impl_t m;
    m.buffer()->assign(TEST_MESSAGE_STARTUP, TEST_MESSAGE_STARTUP + sizeof(TEST_MESSAGE_STARTUP));
    cql::cql_error_t err;
    m.consume(&err);

	BOOST_CHECK_EQUAL(CQL_VERSION_IMPL, m.version());
	BOOST_CHECK_EQUAL("", m.compression());
}

BOOST_AUTO_TEST_SUITE_END()
