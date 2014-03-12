#include <boost/asio.hpp>
#include <boost/test/unit_test.hpp>
#include "cql/cql.hpp"
#include "cql/cql_builder.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/exceptions/cql_no_host_available_exception.hpp"

BOOST_AUTO_TEST_SUITE(cql_builder)

namespace {

void BuildFailure()
{
    boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();

    // We choose a port that Cassandra is NOT running on.
    enum { kPort = 9043 };
    BOOST_REQUIRE_NO_THROW(
        builder->add_contact_point(boost::asio::ip::address::from_string("127.0.0.1"), kPort));

    // Fail to build the cluster.
    boost::shared_ptr<cql::cql_cluster_t> cluster;
    BOOST_REQUIRE_THROW(cluster = builder->build(), cql::cql_no_host_available_exception);
}

}  // namespace

BOOST_AUTO_TEST_CASE(build_failure_race)
{
    // Try this a bunch of times to provoke the race.
    enum { kTries = 1000 };
    for (int i=0; i != kTries; ++i)
        BuildFailure();
}

BOOST_AUTO_TEST_SUITE_END()
