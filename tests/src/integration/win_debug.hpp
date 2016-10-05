/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/
#ifndef __WIN_DEBUG_HPP__
#define __WIN_DEBUG_HPP__

#if defined(_WIN32) && defined(_DEBUG)
# ifdef USE_VISUAL_LEAK_DETECTOR
#   include <vld.h>
# endif
# include <gtest/gtest.h>
#endif

/**
 * Memory leak listener for detecting memory leaks on Windows more efficiently.
 */
class MemoryLeakListener : public testing::EmptyTestEventListener {
public:
#if defined(_WIN32) && defined(_DEBUG)
# ifndef USE_VISUAL_LEAK_DETECTOR
  void OnTestProgramStart(const testing::UnitTest& unit_test);
  void OnTestProgramEnd(const testing::UnitTest& unit_test);
# endif
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
  void check_leaks(const testing::TestInfo& test_information);
#else
  static void disable() {};
  static void enable() {};
#endif
};

#endif // __WIN_DEBUG_HPP__
