/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_UTILS_HPP__
#define __TEST_UTILS_HPP__
#include <iostream>
#include <string>
#include <vector>

#include "exception.hpp"

// Create simple console logging functions
#define PREFIX_LOG std::cout
#define PREFIX_MESSAGE "Integration Tests: "
#define SUFFIX_LOG std::endl
#ifdef INTEGRATION_VERBOSE_LOGGING
# define LOG(message) PREFIX_LOG << PREFIX_MESSAGE << message << SUFFIX_LOG
# define LOG_WARN(message) PREFIX_LOG << PREFIX_MESSAGE << "WARN: " << message << SUFFIX_LOG
#else
# define LOG_DISABLED do {} while (false)
# define LOG(message) LOG_DISABLED
# define LOG_WARN(message) LOG_DISABLED
#endif
#define LOG_ERROR(message) PREFIX_LOG << PREFIX_MESSAGE << "ERROR: " \
                           << __FILE__ << "(" << __LINE__ << "): " \
                           << message << SUFFIX_LOG

namespace test {

/**
 * Base class to provide common integration test functionality
 */
class Utils {
public:
  /**
   * Path separator
   */
  static const char PATH_SEPARATOR;

  /**
   * Get the address of an object
   *
   * @return Address of object
   */
  template <typename T>
  T* addressof(T& value);

  /**
   * Determine if a string contains another string
   *
   * @param input String being evaluated
   * @param search String to find
   * @return True if string is contained in other string; false otherwise
   */
  static bool contains(const std::string& input, const std::string& search);

  /**
   * Get the current working directory
   *
   * @return Current working directory
   */
  static std::string cwd();

  /**
   * Split a string into an array/vector
   *
   * @param input String to convert to array/vector
   * @param delimiter Character to use split into elements (default: <space>)
   * @return An array/vector representation of the string
   */
  static std::vector<std::string> explode(const std::string& input,
    const char delimiter = ' ');

  /**
   * Check to see if a file exists
   *
   * @param filename Absolute/Relative filename
   * @return True if file exists; false otherwise
   */
  static bool file_exists(const std::string& filename);

  /**
   * Indent a string that is delimited by newline characters
   *
   * @param input String to indent
   * @param indent Number of characters/spaces to indent
   * @return Indented string
   */
  static std::string indent(const std::string& input, unsigned int indent);

  /**
   * Concatenate an array/vector into a string
   *
   * @param elements Array/Vector elements to concatenate
   * @param delimiter Character to use between elements (default: <space>)
   * @return A string representation of all the array/vector elements
   */
  static std::string implode(const std::vector<std::string>& elements,
    const char delimiter = ' ');

  /**
   * Create the directory from a path
   *
   * @param path Directory/Path to create
   * @throws test::Exception If there was an issue creating directory
   */
  static void mkdir(const std::string& path);

  /**
   * Cross platform millisecond granularity sleep
   *
   * @param milliseconds Time in milliseconds to sleep
   */
  static void msleep(unsigned int milliseconds);

  /**
   * Replace all occurrences of a string from the input string
   *
   * @param input String having occurrences being replaced
   * @param from String to find for replacement
   * @param to String to replace with
   * @return Input string with replacement
   */
  static std::string replace_all(const std::string& input,
    const std::string& from, const std::string& to);

  /**
   * Reduce/Shorten a multi-line string into a single line string
   *
   * @param input String to reduce/shorten
   * @param add_space_after_newline True if space should be used as a
   *                                replacement for the newline character; false
   *                                otherwise
   * @return Single line string converted from multi-line string
   */
  static std::string shorten(const std::string& input,
    bool add_space_after_newline = true);

  /**
   * Convert a string to lowercase
   *
   * @param input String to convert to lowercase
   */
  static std::string to_lower(const std::string& input);

  /**
   * Remove the leading and trailing whitespace from a string
   *
   * @param input String to trim
   * @return Trimmed string
   */
  static std::string trim(const std::string& input);

  /**
   * Wait for the port on a node to become available
   *
   * @param ip_address IPv4 address of node
   * @param port Port to connect to
   * @param number_of_retries Number of retries to attempt to establish the
   *                          connection on the IP:PORT (DEFAULT: 100)
   * @param retry_delay_ms Retry delay (in milliseconds) before attempting
   *                       reconnection
   * @return True if port on IP address is available; false otherwise
   */
  static bool wait_for_port(const std::string& ip_address, unsigned short port,
    unsigned int number_of_retries = 100,
    unsigned int retry_delay_ms = 100);
};

} // namespace test

#endif //__TEST_UTILS_HPP__
