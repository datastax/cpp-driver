#include <boost/test/unit_test.hpp>
#include "libcql/cql.hpp"
#include "libcql/cql_error.hpp"
#include "libcql/internal/cql_defines.hpp"
#include "libcql/internal/cql_header_impl.hpp"

char TEST_HEADER[] = { 0x01, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x05 };

BOOST_AUTO_TEST_SUITE(cql_header)

BOOST_AUTO_TEST_CASE(getters_and_constructor)
{
    std::stringstream output;
    cql::cql_header_impl_t header(CQL_VERSION_1_REQUEST, CQL_FLAG_NOFLAG, 1, cql::CQL_OPCODE_READY, 5);
    BOOST_CHECK_EQUAL(1, header.version());
    BOOST_CHECK_EQUAL(0, header.flags());
    BOOST_CHECK_EQUAL(1, header.stream());
    BOOST_CHECK_EQUAL(2, header.opcode());
    BOOST_CHECK_EQUAL(5, header.length());
}

BOOST_AUTO_TEST_CASE(setters)
{
    std::stringstream output;
    cql::cql_header_impl_t header;

    header.version(CQL_VERSION_1_REQUEST);
    header.flags(CQL_FLAG_NOFLAG);
    header.stream(1);
    header.opcode(cql::CQL_OPCODE_READY);
    header.length(5);

    BOOST_CHECK_EQUAL(1, header.version());
    BOOST_CHECK_EQUAL(0, header.flags());
    BOOST_CHECK_EQUAL(1, header.stream());
    BOOST_CHECK_EQUAL(2, header.opcode());
    BOOST_CHECK_EQUAL(5, header.length());
}

BOOST_AUTO_TEST_CASE(serialization_size)
{
    cql::cql_header_impl_t header(CQL_VERSION_1_REQUEST, CQL_FLAG_NOFLAG, 0, cql::CQL_OPCODE_READY, 5);
    cql::cql_error_t err;
    header.prepare(&err);
    BOOST_CHECK_EQUAL(8, header.size());
}

BOOST_AUTO_TEST_CASE(serialization_round_trip)
{
    cql::cql_header_impl_t header(CQL_VERSION_1_REQUEST, CQL_FLAG_NOFLAG, 1, cql::CQL_OPCODE_READY, 5);
    cql::cql_error_t err;
    header.prepare(&err);
    header.consume(&err);
    BOOST_CHECK_EQUAL(CQL_VERSION_1_REQUEST, header.version());
    BOOST_CHECK_EQUAL(CQL_FLAG_NOFLAG, header.flags());
    BOOST_CHECK_EQUAL(1, header.stream());
    BOOST_CHECK_EQUAL(cql::CQL_OPCODE_READY, header.opcode());
    BOOST_CHECK_EQUAL(5, header.length());
}

BOOST_AUTO_TEST_CASE(serialization_to_byte)
{
    cql::cql_header_impl_t header(CQL_VERSION_1_REQUEST, CQL_FLAG_NOFLAG, 1, cql::CQL_OPCODE_READY, 5);
    cql::cql_error_t err;
    header.prepare(&err);
    BOOST_CHECK(memcmp(TEST_HEADER, &(*header.buffer())[0], sizeof(TEST_HEADER)) == 0);
}

BOOST_AUTO_TEST_CASE(serialization_from_byte)
{
    cql::cql_header_impl_t header;

    header.buffer()->assign(TEST_HEADER, TEST_HEADER + sizeof(TEST_HEADER));
    cql::cql_error_t err;
    header.consume(&err);
    BOOST_CHECK_EQUAL(1, header.version());
    BOOST_CHECK_EQUAL(0, header.flags());
    BOOST_CHECK_EQUAL(1, header.stream());
    BOOST_CHECK_EQUAL(2, header.opcode());
    BOOST_CHECK_EQUAL(5, header.length());
}

BOOST_AUTO_TEST_SUITE_END()
