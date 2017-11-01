/*
  Copyright (c) DataStax, Inc.

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

BOOST_AUTO_TEST_SUITE(version1_downgrade)

BOOST_AUTO_TEST_CASE(query_after_downgrade)
{
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.0.0" ) {
    test_utils::CassLog::reset("does not support protocol version 2. Trying protocol version 1...");

    size_t row_count;

    {
      test_utils::CassClusterPtr cluster(cass_cluster_new());

      boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
      if (ccm->create_cluster()) {
        ccm->start_cluster();
      }

      test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

      cass_cluster_set_protocol_version(cluster.get(), 2);

      test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

      test_utils::CassResultPtr result;
      test_utils::execute_query(session.get(), "SELECT * FROM system.schema_keyspaces", &result);

      row_count = cass_result_row_count(result.get());
    }

    BOOST_CHECK(row_count > 0);
    BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  } else {
    std::cout << "Invalid Test for Cassandra v" << version.to_string() << ": Use Cassandra v1.2.x to test protocol v1 downgrade" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
