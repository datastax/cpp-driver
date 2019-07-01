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

#define BOOST_TEST_MODULE cassandra

#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstdlib>
#include <string>

/**
 * Destroy all CCM clusters when starting and stopping the integration tests.
 * This will only be run on startup and shutdown using the BOOST_GLOBAL_FIXTURE
 * macro
 */
struct CCMCleanUp {
public:
  CCMCleanUp() {
    std::cout << "Entering C/C++ Driver Integration Test Setup" << std::endl;
    if (!keep_clusters()) {
      CCM::Bridge("config.txt").remove_all_clusters();
    }
  }

  ~CCMCleanUp() {
    std::cout << "Entering C/C++ Driver Integration Test Teardown" << std::endl;
    if (!keep_clusters()) {
      CCM::Bridge("config.txt").remove_all_clusters();
    }
  }

  bool keep_clusters() {
    std::string value;
    char* temp = getenv("KEEP_CLUSTERS");
    if (temp) {
      value.assign(temp);
      std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    }
    return !value.empty() && value != "0" && value != "false";
  }
};
BOOST_GLOBAL_FIXTURE(CCMCleanUp);

/**
 * Enable test case messages (display current test case being run)
 */
struct EnableTestCaseOutput {
  EnableTestCaseOutput() {
    boost::unit_test::unit_test_log_t::instance().set_threshold_level(
        boost::unit_test::log_test_units);
  }
};
BOOST_GLOBAL_FIXTURE(EnableTestCaseOutput);

using test_utils::CassLog;
BOOST_GLOBAL_FIXTURE(CassLog);
