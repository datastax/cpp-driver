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

#include "cassandra.h"
#include "ssl.hpp"
#include "string.hpp"

using datastax::String;
using datastax::internal::core::SslContextFactory;

#if defined(_WIN32) && defined(_DEBUG)
#ifdef USE_VISUAL_LEAK_DETECTOR
#include <vld.h>
#else
// Enable memory leak detection
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#include <stdlib.h>

// Enable memory leak detection for new operator
#ifndef DBG_NEW
#define DBG_NEW new (_NORMAL_BLOCK, __FILE__, __LINE__)
#define new DBG_NEW
#endif

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
#endif

/**
 * Memory leak listener for detecting memory leaks on Windows more efficiently.
 */
class MemoryLeakListener : public testing::EmptyTestEventListener {
public:
#ifndef USE_VISUAL_LEAK_DETECTOR
  void OnTestProgramStart(const testing::UnitTest& unit_test) {
    // Install the memory leak reporting
    _CrtSetReportHook2(_CRT_RPTHOOK_INSTALL, &output_memory_leak_results);
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    // Uninstall/Remove the memory leak reporting
    _CrtSetReportHook2(_CRT_RPTHOOK_REMOVE, &output_memory_leak_results);
  }
#endif

  void OnTestStart(const testing::TestInfo& test_information) {
#ifdef USE_VISUAL_LEAK_DETECTOR
    VLDMarkAllLeaksAsReported();
    VLDEnable();
#else
    // Get the starting memory state
    _CrtMemCheckpoint(&memory_start_state_);
#endif
  }

  void OnTestEnd(const testing::TestInfo& test_information) {
    // Check for memory leaks if the test was successful
    if (test_information.result()->Passed()) {
      check_leaks(test_information);
    }
  }

private:
#ifndef USE_VISUAL_LEAK_DETECTOR
  /**
   * Starting memory state (before start of test)
   */
  _CrtMemState memory_start_state_;
#endif

  /**
   * Check for memory leaks based on the starting memory state
   *
   * @param test_information Information about the test
   */
  void check_leaks(const testing::TestInfo& test_information) {
#ifdef USE_VISUAL_LEAK_DETECTOR
    // Determine if a difference exists (e.g. leak)
    VLDDisable();
    if (VLDGetLeaksCount() > 0) {
      VLDReportLeaks();
      VLDMarkAllLeaksAsReported();
#else
    // Get the ending memory state for the test
    _CrtMemState memory_end_state;
    _CrtMemCheckpoint(&memory_end_state);
    _CrtMemState memory_state_difference;

    // Determine if a difference exists (e.g. leak)
    if (_CrtMemDifference(&memory_state_difference, &memory_start_state_, &memory_end_state)) {
      _CrtMemDumpStatistics(&memory_state_difference);
#endif
      FAIL() << "Memory leaks detected in " << test_information.test_case_name() << "."
             << test_information.name();
    }
  }
};
#endif

/**
 * Bootstrap listener for handling start and end of the unit tests.
 */
class BootstrapListener : public testing::EmptyTestEventListener {
  void OnTestProgramStart(const testing::UnitTest& unit_test) {
    std::cout << "Starting DataStax C/C++ Driver Unit Test" << std::endl;
    std::cout << "  v" << CASS_VERSION_MAJOR << "." << CASS_VERSION_MINOR << "."
              << CASS_VERSION_PATCH;
    if (!String(CASS_VERSION_SUFFIX).empty()) {
      std::cout << "-" << CASS_VERSION_SUFFIX;
    }
    std::cout << std::endl;
  }

  void OnTestProgramEnd(const testing::UnitTest& unit_test) {
    // TODO: Handle the ending of unit tests
    std::cout << "Finishing DataStax C/C++ Driver Unit Test" << std::endl;
  }

  void OnTestStart(const testing::TestInfo& test_information) { SslContextFactory::init(); }

  void OnTestEnd(const testing::TestInfo& test_information) { SslContextFactory::cleanup(); }
};

int main(int argc, char* argv[]) {
  // Initialize the Google testing framework
  testing::InitGoogleTest(&argc, argv);

  // Add listeners for program start and finish events
  testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();

#if defined(_WIN32) && defined(_DEBUG)
  // Add the memory leak checking to the listener callbacks
  listeners.Append(new MemoryLeakListener());
#ifdef USE_VISUAL_LEAK_DETECTOR
  // Google test statically initializes heap objects; mark all leaks as reported
  VLDMarkAllLeaksAsReported();
#endif
#endif

  listeners.Append(new BootstrapListener());

  // Run the unit tests
  return RUN_ALL_TESTS();
}
