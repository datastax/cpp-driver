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

#if defined(_WIN32) && defined(_DEBUG)
#include "win_debug.hpp"

#ifndef USE_VISUAL_LEAK_DETECTOR
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
inline int __cdecl output_memory_leak_results(int report_type, char* message, int* error_code) {
  std::cerr << message;
  return 1;
}
#endif

void MemoryLeakListener::disable() {
#ifdef USE_VISUAL_LEAK_DETECTOR
  VLDDisable();
#else
  int flags = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
  flags &= ~_CRTDBG_ALLOC_MEM_DF;
  _CrtSetDbgFlag(flags);
#endif
}

void MemoryLeakListener::enable() {
#ifdef USE_VISUAL_LEAK_DETECTOR
  VLDEnable();
#else
  int flags = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
  flags |= _CRTDBG_ALLOC_MEM_DF;
  _CrtSetDbgFlag(flags);
#endif
}

#ifndef USE_VISUAL_LEAK_DETECTOR
void MemoryLeakListener::OnTestProgramStart(const testing::UnitTest& unit_test) {
  // Install the memory leak reporting
  _CrtSetReportHook2(_CRT_RPTHOOK_INSTALL, &output_memory_leak_results);
}

void MemoryLeakListener::OnTestProgramEnd(const testing::UnitTest& unit_test) {
  // Uninstall/Remove the memory leak reporting
  _CrtSetReportHook2(_CRT_RPTHOOK_REMOVE, &output_memory_leak_results);
}
#endif

void MemoryLeakListener::OnTestStart(const testing::TestInfo& test_information) {
#ifdef USE_VISUAL_LEAK_DETECTOR
  // Make all memory leaks (if any) as reported (clean slate)
  VLDMarkAllLeaksAsReported();
#else
  // Get the starting memory state
  _CrtMemCheckpoint(&memory_start_state_);
#endif

  // Enable memory leak detection
  enable();
}

void MemoryLeakListener::OnTestEnd(const testing::TestInfo& test_information) {
  // Check for memory leaks if the test was successful
  if (test_information.result()->Passed()) {
    check_leaks(test_information);
  }
}

void MemoryLeakListener::check_leaks(const testing::TestInfo& test_information) {
  // Disable memory leak checking
  disable();

#ifdef USE_VISUAL_LEAK_DETECTOR
  // Determine if a difference exists (e.g. leak)
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
    _CrtMemDumpAllObjectsSince(&memory_start_state_);
    _CrtMemDumpStatistics(&memory_state_difference);
#endif
    FAIL() << "Memory leaks detected in " << test_information.test_case_name() << "."
           << test_information.name();
  }
}

#endif
