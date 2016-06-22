/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <gtest/gtest.h>

#include "bridge.hpp"
#include "options.hpp"
#include "win_debug.hpp"

#include "dse.h"

#include <ostream>

/**
 * Bootstrap listener for handling start and end of the integration tests.
 */
class BootstrapListener : public testing::EmptyTestEventListener {
  void OnTestProgramStart(const testing::UnitTest& unit_test) {
    std::cout << "Starting DataStax C/C++ Driver Integration Test" << std::endl;
    std::cout << "  Cassandra driver v"
              << CASS_VERSION_MAJOR << "."
              << CASS_VERSION_MINOR << "."
              << CASS_VERSION_PATCH;
    if (!std::string(CASS_VERSION_SUFFIX).empty()) {
      std::cout << "-" << CASS_VERSION_SUFFIX;
    }
    std::cout << std::endl << "  DSE driver v"
              << DSE_VERSION_MAJOR << "."
              << DSE_VERSION_MINOR << "."
              << DSE_VERSION_PATCH;
    if (!std::string(DSE_VERSION_SUFFIX).empty()) {
      std::cout << "-" << DSE_VERSION_SUFFIX;
    }
    std::cout << std::endl;
    Options::print_settings();
    Options::ccm()->remove_all_clusters();
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    std::cout << "Finishing DataStax C/C++ Driver Integration Test" << std::endl;
    Options::ccm()->remove_all_clusters();
  }
};

int main(int argc, char* argv[]) {
  // Initialize the Google testing framework
  testing::InitGoogleTest(&argc, argv);

  // Add a bootstrap mechanism for program start and finish
  testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();
  listeners.Append(new BootstrapListener());

#if defined(_WIN32) && defined(_DEBUG)
  // Add the memory leak checking to the listener callbacks
  listeners.Append(new MemoryLeakListener());
# ifdef USE_VISUAL_LEAK_DETECTOR
  // Google test statically initializes heap objects; mark all leaks as reported
  VLDMarkAllLeaksAsReported();
# endif
#endif

  // Initial the options for the integration test
  Options::initialize(argc, argv);

  // Run the integration tests
  return RUN_ALL_TESTS();
}
