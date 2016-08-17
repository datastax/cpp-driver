/*
  Copyright (c) 2014-2016 DataStax

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

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

#include <map>
#include <set>
#include <string>

BOOST_AUTO_TEST_SUITE(custom_payload)

BOOST_AUTO_TEST_CASE(simple)
{
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
    if (ccm->create_cluster()) {
      // Ensure the cluster is down before updating JVM argument
      ccm->kill_cluster();
    }
    ccm->start_cluster("-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler");

    test_utils::CassClusterPtr cluster(cass_cluster_new());
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));

    test_utils::CassCustomPayloadPtr custom_payload(cass_custom_payload_new());

    typedef std::map<std::string, std::string> PayloadItemMap;

    PayloadItemMap items;
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    for (PayloadItemMap::const_iterator i = items.begin(); i != items.end(); ++i) {
      cass_custom_payload_set(custom_payload.get(),
                              i->first.c_str(),
                              reinterpret_cast<const cass_byte_t*>(i->second.data()), i->second.size());
    }

    cass_statement_set_custom_payload(statement.get(), custom_payload.get());

    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

    size_t item_count = cass_future_custom_payload_item_count(future.get());

    BOOST_REQUIRE(item_count == items.size());

    for (size_t i = 0; i < item_count; ++i) {
      const char* name;
      size_t name_length;
      const cass_byte_t* value;
      size_t value_size;
      BOOST_REQUIRE(cass_future_custom_payload_item(future.get(), i,
                                                    &name, &name_length,
                                                    &value, &value_size) == CASS_OK);

      PayloadItemMap::const_iterator it = items.find(std::string(name, name_length));
      BOOST_REQUIRE(it != items.end());
      BOOST_CHECK_EQUAL(it->second, std::string(reinterpret_cast<const char*>(value), value_size));

      // Ensure the cluster is down before (updated JVM argument)
      ccm->kill_cluster();
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping custom_payload/simple" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
