/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/atomic.hpp>
#include <boost/scoped_ptr.hpp>

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

struct Version1DowngradeTests {
  Version1DowngradeTests() {}
};

BOOST_FIXTURE_TEST_SUITE(version1_downgrade, Version1DowngradeTests)

BOOST_AUTO_TEST_CASE(query_after_downgrade)
{
  test_utils::CassLog::reset("Error response: 'Invalid or unsupported protocol version: 2");

  size_t row_count;

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration("config_v1.txt");

    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 3, 0);

    cass_cluster_set_protocol_version(cluster.get(), 2);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    test_utils::CassResultPtr result;
    test_utils::execute_query(session.get(), "SELECT * FROM system.schema_keyspaces", &result);

    row_count = cass_result_row_count(result.get());
  }

  BOOST_CHECK(row_count > 0);
  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

BOOST_AUTO_TEST_SUITE_END()
