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

#ifndef __WIN_DEBUG_HPP__
#define __WIN_DEBUG_HPP__

#if defined(_WIN32) && defined(_DEBUG)
#ifdef USE_VISUAL_LEAK_DETECTOR
#include <vld.h>
#endif
#include <gtest/gtest.h>
#endif

/**
 * Memory leak listener for detecting memory leaks on Windows more efficiently.
 */
class MemoryLeakListener : public testing::EmptyTestEventListener {
public:
#if defined(_WIN32) && defined(_DEBUG)
#ifndef USE_VISUAL_LEAK_DETECTOR
  void OnTestProgramStart(const testing::UnitTest& unit_test);
  void OnTestProgramEnd(const testing::UnitTest& unit_test);
#endif
  void OnTestStart(const testing::TestInfo& test_information);
  void OnTestEnd(const testing::TestInfo& test_information);

  /**
   * Disable memory leak detection
   */
  static void disable();
  /**
   * Enable memory leak detection
   */
  static void enable();

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
  void check_leaks(const testing::TestInfo& test_information);
#else
  static void disable(){};
  static void enable(){};
#endif
};

#endif // __WIN_DEBUG_HPP__
