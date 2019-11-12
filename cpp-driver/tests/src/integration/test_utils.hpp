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

#ifndef __TEST_UTILS_HPP__
#define __TEST_UTILS_HPP__
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "cassandra.h"
#include "exception.hpp"

#define ARRAY_LEN(array) (sizeof(array) / sizeof(array[0]))

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
   * Get the CQL type from the value type
   *
   * @param value_type Scalar value type; others are ignored
   * @throws Exception If scalar CQL type cannot be determined
   */
  static std::string scalar_cql_type(CassValueType value_type);

  /**
   * Split a string into an array/vector
   *
   * @param input String to convert to array/vector
   * @param delimiter Character to use split into elements (default: <space>)
   * @return An array/vector representation of the string
   */
  static std::vector<std::string> explode(const std::string& input, const char delimiter = ' ');

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
   * Concatenate or join a vector into a string
   *
   * @param elements Vector of elements to concatenate
   * @param delimiter Character to use between elements (default: <space>)
   * @return A string concatenating all the vector elements with delimiter
   *         separation
   */
  template <typename T>
  inline static std::string implode(const std::vector<T>& elements, const char delimiter = ' ') {
    // Iterate through each element in the vector and concatenate the string
    std::stringstream result;
    for (typename std::vector<T>::const_iterator iterator = elements.begin();
         iterator < elements.end(); ++iterator) {
      result << *iterator;
      if ((iterator + 1) != elements.end()) {
        result << delimiter;
      }
    }
    std::string temp = result.str();
    return temp;
  }

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
  static std::string replace_all(const std::string& input, const std::string& from,
                                 const std::string& to);

  /**
   * Reduce/Shorten a multi-line string into a single line string
   *
   * @param input String to reduce/shorten
   * @param add_space_after_newline True if space should be used as a
   *                                replacement for the newline character; false
   *                                otherwise
   * @return Single line string converted from multi-line string
   */
  static std::string shorten(const std::string& input, bool add_space_after_newline = true);

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

  /**
   * Get the home directory for the current user (not thread safe)
   *
   * @return Home directory
   */
  static std::string home_directory();

  /**
   * Get the temporary directory for the current operating system (not thread safe)
   *
   * return Temporary directory
   */
  static std::string temp_directory();
};

} // namespace test

#endif //__TEST_UTILS_HPP__
