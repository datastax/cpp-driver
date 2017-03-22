/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_LOGGER_HPP__
#define __TEST_LOGGER_HPP__
#include "objects.hpp"

#include "macros.hpp"

#include <uv.h>

#include <fstream>
#include <string>
#include <vector>

namespace test {
namespace driver {

/**
 * Logger class for handling log messages from the driver
 */
class Logger {
public:
  /**
   * Create the logger
   */
  Logger();

  /**
   * Clean up the logger (e.g. close and flush the file)
   */
  ~Logger();

  /**
   * Initialize the driver logging callback
   *
   * @param test_case_name Name of the test case being run
   * @param test_name Name of the test being run
   */
  void initialize(const std::string& test_case_name, const std::string& test_name);

  /**
   * Add criteria to the search criteria for incoming log messages
   *
   * @param criteria Criteria to add
   */
  void add_critera(const std::string& criteria);

  /**
   * Clear criteria from the search criteria for incoming log messages
   */
  void clear_critera();

  /**
   * Get the number of log messages that matched the search criteria
   *
   * @return Number of matched log messages
   */
  size_t get_count();

  /**
   * Clear the logging criteria and reset the count
   */
  void reset();

private:
  /**
   * Mutex for operations on the logging callback
   */
  static uv_mutex_t mutex_;
  /**
   * Logging file stream to output driver logging messages
   */
  std::fstream output_;
  /**
   * List of search criteria to match incoming log messages
   */
  static std::vector<std::string> search_criteria_;
  /**
   * Number of log messages that match the search criteria
   */
  static size_t count_;

  /**
   * Log the message from the driver (callback)
   *
   * @param log Log message structure from the driver
   * @param data Object passed from the driver (NULL or std::fstream)
   */
  static void log(const CassLogMessage* log, void* data);
};

} // namespace driver
} // namespace test

#endif // __TEST_LOGGER_HPP__
