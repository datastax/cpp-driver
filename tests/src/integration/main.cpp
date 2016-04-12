#include <gtest/gtest.h>

#include "bridge.hpp"
#include "options.hpp"

#include "dse.h"

#if defined(_WIN32) && defined(_DEBUG)
# ifdef USE_VISUAL_LEAK_DETECTOR
#   include <vld.h>
# else
// Enable memory leak detection
# define _CRTDBG_MAP_ALLOC
# include <stdlib.h>
# include <crtdbg.h>

// Enable memory leak detection for new operator
# ifndef DBG_NEW
#   define DBG_NEW new ( _NORMAL_BLOCK , __FILE__ , __LINE__ )
#   define new DBG_NEW
# endif

/**
 * Output the memory leak results to the console
 *
 * @param report_type Type of report (warn, error, or assert)
 * @param message message
 * @param error_code Error code
 * @return Result to return to CRT processing (1 will stop processing report)
 */
int __cdecl output_memory_leak_results(int report_type, char* message, int* error_code) {
  std::cerr << message;
  return 1;
}
# endif

/**
 * Memory leak listener for detecting memory leaks on Windows more efficiently.
 */
class MemoryLeakListener : public testing::EmptyTestEventListener {
public:

# ifndef USE_VISUAL_LEAK_DETECTOR
  void OnTestProgramStart(const testing::UnitTest& unit_test) {
    // Install the memory leak reporting
    _CrtSetReportHook2(_CRT_RPTHOOK_INSTALL, &output_memory_leak_results);
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    // Uninstall/Remove the memory leak reporting
    _CrtSetReportHook2(_CRT_RPTHOOK_REMOVE, &output_memory_leak_results);
  }
# endif

  void OnTestStart(const testing::TestInfo& test_information) {
# ifdef USE_VISUAL_LEAK_DETECTOR
    VLDMarkAllLeaksAsReported();
    VLDEnable();
# else
    // Get the starting memory state
    _CrtMemCheckpoint(&memory_start_state_);
# endif
  }

  void OnTestEnd(const testing::TestInfo& test_information) {
    // Check for memory leaks if the test was successful
    if(test_information.result()->Passed()) {
      check_leaks(test_information);
    }
}

private:
# ifndef USE_VISUAL_LEAK_DETECTOR
  /**
   * Starting memory state (before start of test)
   */
  _CrtMemState memory_start_state_;
# endif

  /**
   * Check for memory leaks based on the starting memory state
   *
   * @param test_information Information about the test
   */
  void check_leaks(const testing::TestInfo& test_information) {
# ifdef USE_VISUAL_LEAK_DETECTOR
    // Determine if a difference exists (e.g. leak)
    VLDDisable();
    if (VLDGetLeaksCount() > 0) {
      VLDReportLeaks();
      VLDMarkAllLeaksAsReported();
# else
    // Get the ending memory state for the test
    _CrtMemState memory_end_state;
    _CrtMemCheckpoint(&memory_end_state);
    _CrtMemState memory_state_difference;

    // Determine if a difference exists (e.g. leak)
    if (_CrtMemDifference(&memory_state_difference, &memory_start_state_, &memory_end_state)) {
      _CrtMemDumpStatistics(&memory_state_difference);
# endif
      FAIL() << "Memory leaks detected in "
             << test_information.test_case_name()
             << "." << test_information.name();
    }
  }
};
#endif

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
    CCM::Bridge(Options::server_version(), Options::use_git(),
      Options::is_dse(), Options::cluster_prefix(),
      Options::dse_credentials(),
      Options::dse_username(), Options::dse_password(),
      Options::deployment_type(), Options::authentication_type(),
      Options::host(), Options::port(),
      Options::username(), Options::password(),
      Options::public_key(), Options::private_key()).remove_all_clusters();
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    std::cout << "Finishing DataStax C/C++ Driver Integration Test" << std::endl;
    CCM::Bridge(Options::server_version(), Options::use_git(),
      Options::is_dse(), Options::cluster_prefix(),
      Options::dse_credentials(),
      Options::dse_username(), Options::dse_password(),
      Options::deployment_type(), Options::authentication_type(),
      Options::host(), Options::port(),
      Options::username(), Options::password(),
      Options::public_key(), Options::private_key()).remove_all_clusters();
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
