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

#include <gtest/gtest.h>

#include "bridge.hpp"
#include "options.hpp"
#include "ssl.hpp"
#include "win_debug.hpp"

#include "cassandra.h"
#include "test_utils.hpp"

#include <ostream>

using datastax::internal::core::SslContextFactory;

/**
 * Bootstrap listener for handling start and end of the integration tests.
 */
class BootstrapListener : public testing::EmptyTestEventListener {
public:
  BootstrapListener()
      : is_settings_displayed_(false) {}

  /**
   * Set the current test category being executed
   *
   * @param category Current category
   */
  void set_category(TestCategory category) { category_ = category; }

  void OnTestProgramStart(const testing::UnitTest& unit_test) {
    if (!is_settings_displayed_) {
      std::cout << "Starting DataStax C/C++ Driver Integration Test" << std::endl;
      std::cout << "  v" << CASS_VERSION_MAJOR << "." << CASS_VERSION_MINOR << "."
                << CASS_VERSION_PATCH;
      if (!std::string(CASS_VERSION_SUFFIX).empty()) {
        std::cout << "-" << CASS_VERSION_SUFFIX;
      }
      std::cout << std::endl << "  libuv v" << uv_version_string() << std::endl;
      Options::print_settings();
      is_settings_displayed_ = true;
    }
    if (!Options::keep_clusters()) {
      Options::ccm()->remove_all_clusters();
    }
    std::cout << "Category: " << category_ << std::endl;
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    std::cout << std::endl;
    if (!Options::keep_clusters()) {
      Options::ccm()->remove_all_clusters();
    }
  }

  void OnTestStart(const testing::TestInfo& test_information) { SslContextFactory::init(); }

  void OnTestEnd(const testing::TestInfo& test_information) { SslContextFactory::cleanup(); }

private:
  /**
   * Current category
   */
  TestCategory category_;
  /**
   * Flag to determine if the settings have been displayed
   */
  bool is_settings_displayed_;
};

/**
 * Generate the entire filter pattern which includes the base filter applied and
 * the exclusion filter based on the given category
 *
 * @param category Category that should be enabled
 * @param base_filter Base filter being applied to exclusion
 * @return Filter pattern to execute for the given category
 */
std::string generate_filter(TestCategory category, const std::string& base_filter) {
  // Create the exclusion filter by iterating over the available categories
  std::string exclude_filter;
  for (TestCategory::iterator iterator = TestCategory::begin(); iterator != TestCategory::end();
       ++iterator) {
    if (category != *iterator) {
      exclude_filter += ":" + iterator->filter();
    }
  }

  // Determine if the negative pattern should be applied
  if (!exclude_filter.empty() && !test::Utils::contains(base_filter, "-")) {
    exclude_filter.insert(1, "-");
  }

  //
  return base_filter + exclude_filter;
}

int main(int argc, char* argv[]) {
  // Initialize the Google testing framework
  testing::InitGoogleTest(&argc, argv);
  testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();

#if defined(_WIN32) && defined(_DEBUG)
  // Add the memory leak checking to the listener callbacks
  listeners.Append(new MemoryLeakListener());
#ifdef USE_VISUAL_LEAK_DETECTOR
  // Google test statically initializes heap objects; mark all leaks as reported
  VLDMarkAllLeaksAsReported();
#endif
#endif

  // Add a bootstrap mechanism for program start and finish
  BootstrapListener* listener = NULL;
  listeners.Append(listener = new BootstrapListener());

  // Initialize the options for the integration test
  if (Options::initialize(argc, argv)) {
    // Run the integration tests from each applicable category
    int exit_status = 0;
    const std::string base_filter = ::testing::GTEST_FLAG(filter);
    std::set<TestCategory> categories = Options::categories();
    for (std::set<TestCategory>::iterator iterator = categories.begin();
         iterator != categories.end(); ++iterator) {
      // Update the filtering based on the current category
      ::testing::GTEST_FLAG(filter) = generate_filter(*iterator, base_filter);
      listener->set_category(*iterator);

      // Execute the current category and determine if a failure occurred
      if (0 != RUN_ALL_TESTS()) {
        exit_status = 1;
      }
    }
    // TODO: Write a custom start and end report for all categories executed
    std::cout << "Finishing DataStax C/C++ Driver Integration Test" << std::endl;
    return exit_status;
  }
  return Options::is_help() ? 0 : 1;
}
