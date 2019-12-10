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

#include "logger.hpp"
#include "integration.hpp"

#include "cassandra.h"
#include "scoped_lock.hpp"

#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace datastax::internal;
using namespace test::driver;

#define LOGGER_DIRECTORY "log"
#ifdef _WIN32
#define FILE_MODE 0
#define LOCALTIME(tm, time) localtime_s(tm, time)
#else
#define FILE_MODE S_IRWXU | S_IRWXG | S_IROTH
#define LOCALTIME(tm, time) localtime_r(time, tm)
#endif

Logger::Logger()
    : count_(0) {}

Logger::~Logger() {
  if (output_.is_open()) {
    output_.close();
  }
}

void Logger::initialize(const std::string& test_case, const std::string& test_name) {
  // Create the logger directory
  std::string path = std::string(LOGGER_DIRECTORY) + Utils::PATH_SEPARATOR + test_case;
  path = Utils::replace_all(path, "_", std::string(1, Utils::PATH_SEPARATOR));
  std::vector<std::string> path_tokens = Utils::explode(path, Utils::PATH_SEPARATOR);
  std::string mkdir_path;
  for (std::vector<std::string>::const_iterator iterator = path_tokens.begin();
       iterator < path_tokens.end(); ++iterator) {
    mkdir_path += *iterator + Utils::PATH_SEPARATOR;
    Utils::mkdir(mkdir_path);
  }

  // Create the relative file name for the test and the associated stream
  std::string filename = path + Utils::PATH_SEPARATOR + test_name + ".log";
  output_.open(filename.c_str(), std::fstream::out | std::fstream::trunc);

  // Initialize the driver logger callback
  if (output_.fail()) {
    TEST_LOG_ERROR("Unable to Create Log File: " << filename);
  }

  // Create the mutex for callback operations (thread safety)
  uv_mutex_init(&mutex_);

  // Set the maximum driver log level to capture all logs messages
  cass_log_set_level(CASS_LOG_TRACE);
  cass_log_set_callback(Logger::log, this);
}

void Logger::add_critera(const std::string& criteria) {
  ScopedMutex lock(&mutex_);
  search_criteria_.push_back(criteria);
}

void test::driver::Logger::clear_critera() {
  ScopedMutex lock(&mutex_);
  search_criteria_.clear();
}

size_t Logger::count() { return count_; }

void Logger::log(const CassLogMessage* log, void* data) {
  Logger* logger = static_cast<Logger*>(data);
  ScopedMutex lock(&(logger->mutex_));

  // Get the log message
  std::string message = log->message;

  // Convert the epoch to human readable format date/time
  cass_uint64_t epoch_seconds = log->time_ms / 1000;
  unsigned short milliseconds = log->time_ms % 1000;
  time_t rawtime = static_cast<time_t>(epoch_seconds);
  struct tm date_time;
  LOCALTIME(&date_time, &rawtime);

  // Create the date formatted output
  std::stringstream date;
  int month = (date_time.tm_mon + 1);
  date << date_time.tm_year + 1900 << "/" << (month < 10 ? "0" : "") << month << "/"
       << (date_time.tm_mday < 10 ? "0" : "") << date_time.tm_mday;

  // Create the formatted time
  std::stringstream time;
  time << (date_time.tm_hour < 10 ? "0" : "") << date_time.tm_hour << ":"
       << (date_time.tm_min < 10 ? "0" : "") << date_time.tm_min << ":"
       << (date_time.tm_sec < 10 ? "0" : "") << date_time.tm_sec << "." << std::setfill('0')
       << std::setw(3) << milliseconds;

  // Create the formatted log message and output to the file
  std::string severity = cass_log_level_string(log->severity);
  logger->output_ << date.str() << " " << time.str() << " " << severity << ": " << message << " ("
                  << log->file << ":" << log->line << ") " << std::endl;

  // Determine if the log message matches any of the criteria
  for (std::vector<std::string>::const_iterator iterator = logger->search_criteria_.begin();
       iterator != logger->search_criteria_.end(); ++iterator) {
    if (message.find(*iterator) != std::string::npos) {
      ++logger->count_;
    }
  }
}

void test::driver::Logger::reset() {
  ScopedMutex lock(&mutex_);
  search_criteria_.clear();
  count_ = 0;
}

void test::driver::Logger::reset_count() {
  ScopedMutex lock(&mutex_);
  count_ = 0;
}
