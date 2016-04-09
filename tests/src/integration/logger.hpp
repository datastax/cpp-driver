/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __LOGGER_HPP__
#define __LOGGER_HPP__
#include "objects.hpp"

#include <uv.h>

#include <fstream>
#include <string>
#include <vector>

namespace driver {

  /**
   * Logger class for handling log messages from the driver
   */
  class Logger {
  public:
    /**
     * Create the logger without writing the log messages from the driver to a
     * file
     */
    Logger();

    /**
     * Create the logger and write the log messages from the driver to a file
     *
     * @param test_case_name Name of the test case being run
     * @param test_name Name of the test being run
     */
    Logger(const std::string& test_case_name, const std::string& test_name);

    /**
     * Add criteria to the search criteria for incoming log messages
     *
     * @param criteria Criteria to add
     */
    void add_critera(const std::string& criteria);

    /**
     * Get the number of log messages that matched the search criteria
     *
     * @return Number of matched log messages
     */
    size_t get_count();

  private:
    /**
     * Deleter class for the object std::fstream
     */
    class FStreamDeleter {
    public:
      void operator()(std::fstream* stream) {
        if (stream) {
          stream->close();
          delete stream;
        }
      }
    };

    /**
     * Mutex for operations on the logging callback
     */
    static uv_mutex_t mutex_;
    /**
     * Logging file stream to output driver logging messages
     */
    SmartPtr<std::fstream, FStreamDeleter> output_;
    /**
     * List of search criteria to match incoming log messages
     */
    static std::vector<std::string> search_criteria_;
    /**
     * Number of log messages that match the search criteria
     */
    static size_t count_;

    /**
     * Create the directory from a path
     *
     * @param path Directory/Path to create
     */
    void mkdir(std::string path);

    /**
     * Initialize the driver logging callback
     *
     * @param data Object to pass with the callback from the driver
     *             (default: NULL)
     */
    void initialize(std::fstream* data = NULL);

    /**
     * Log the message from the driver (callback)
     *
     * @param log Log message structure from the driver
     * @param data Object passed from the driver (NULL or std::fstream)
     */
    static void log(const CassLogMessage* log, void* data);
  };
}

#endif // __LOGGER_HPP__