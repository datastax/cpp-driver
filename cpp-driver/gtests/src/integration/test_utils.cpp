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

#include "test_utils.hpp"

#include "exception.hpp"
#include "tsocket.hpp"

#include <uv.h>

#include <algorithm>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#ifndef _WIN32
#include <time.h>
#endif

#ifdef _WIN32
#define FILE_MODE 0
#else
#define FILE_MODE S_IRWXU | S_IRWXG | S_IROTH
#endif
#define FILE_PATH_SIZE 1024

#define TRIM_DELIMETERS " \f\n\r\t\v"

// Constant and static initialization
#ifdef _WIN32
const char test::Utils::PATH_SEPARATOR = '\\';
#else
const char test::Utils::PATH_SEPARATOR = '/';
#endif

template <typename T>
T* test::Utils::addressof(T& value) {
  return reinterpret_cast<T*>(&const_cast<char&>(reinterpret_cast<const volatile char&>(value)));
}

bool test::Utils::contains(const std::string& input, const std::string& search) {
  return input.find(search) != std::string::npos ? true : false;
}

std::string test::Utils::cwd() {
  char cwd[FILE_PATH_SIZE] = { 0 };
  size_t cwd_length = sizeof(cwd);
  uv_cwd(cwd, &cwd_length);
  return std::string(cwd, cwd_length);
}

std::string test::Utils::scalar_cql_type(CassValueType value_type) {
  switch (value_type) {
    case CASS_VALUE_TYPE_ASCII:
      return "ascii";
      break;
    case CASS_VALUE_TYPE_BIGINT:
      return "bigint";
      break;
    case CASS_VALUE_TYPE_BLOB:
      return "blob";
      break;
    case CASS_VALUE_TYPE_BOOLEAN:
      return "boolean";
      break;
    case CASS_VALUE_TYPE_COUNTER:
      return "counter";
      break;
    case CASS_VALUE_TYPE_DECIMAL:
      return "decimal";
      break;
    case CASS_VALUE_TYPE_DOUBLE:
      return "double";
      break;
    case CASS_VALUE_TYPE_FLOAT:
      return "float";
      break;
    case CASS_VALUE_TYPE_INT:
      return "int";
      break;
    case CASS_VALUE_TYPE_TEXT:
      return "text";
      break;
    case CASS_VALUE_TYPE_TIMESTAMP:
      return "timestamp";
      break;
    case CASS_VALUE_TYPE_UUID:
      return "uuid";
      break;
    case CASS_VALUE_TYPE_VARCHAR:
      return "varchar";
      break;
    case CASS_VALUE_TYPE_VARINT:
      return "varint";
      break;
    case CASS_VALUE_TYPE_TIMEUUID:
      return "timeuuid";
      break;
    case CASS_VALUE_TYPE_INET:
      return "inet";
      break;
    case CASS_VALUE_TYPE_DATE:
      return "date";
      break;
    case CASS_VALUE_TYPE_TIME:
      return "time";
      break;
    case CASS_VALUE_TYPE_SMALL_INT:
      return "smallint";
      break;
    case CASS_VALUE_TYPE_TINY_INT:
      return "tinyint";
      break;
    case CASS_VALUE_TYPE_DURATION:
      return "duration";
      break;
    default:
      std::stringstream message;
      message << "Unable to Retrieve CQL Type: CassValueType [" << value_type << "] is not valid";
      throw test::Exception(message.str());
  }
}

std::vector<std::string> test::Utils::explode(const std::string& input,
                                              const char delimiter /*= ' '*/) {
  // Iterate over the input line and parse the tokens
  std::vector<std::string> result;
  std::istringstream parser(input);
  for (std::string token; std::getline(parser, token, delimiter);) {
    if (!token.empty()) {
      result.push_back(trim(token));
    }
  }
  return result;
}

bool test::Utils::file_exists(const std::string& filename) {
  uv_fs_t request;
  int error_code = uv_fs_stat(NULL, &request, filename.c_str(), NULL);
  uv_fs_req_cleanup(&request);
  return error_code == 0;
}

std::string test::Utils::indent(const std::string& input, unsigned int indent) {
  std::stringstream output;

  // Iterate over each line in the input string and indent
  std::vector<std::string> lines = explode(input, '\n');
  for (std::vector<std::string>::iterator iterator = lines.begin(); iterator < lines.end();
       ++iterator) {
    output << std::setw(indent) << "" << *iterator;
    if ((iterator + 1) != lines.end()) {
      output << std::endl;
    }
  }
  return output.str();
}

void test::Utils::mkdir(const std::string& path) {
  // Create a synchronous libuv file system call to create the path
  uv_loop_t loop;
  uv_loop_init(&loop);
  uv_fs_t request;
  int error_code;
  std::string error_message;
  error_code = uv_fs_mkdir(&loop, &request, path.c_str(), FILE_MODE, NULL);
  if (error_code != 0 && error_code != UV_EEXIST) {
    error_message = uv_strerror(error_code);
  }
  uv_run(&loop, UV_RUN_DEFAULT);
  uv_loop_close(&loop);
  uv_fs_req_cleanup(&request);

  // Determine if there was an issue creating the directory
  if (!error_message.empty()) {
    throw test::Exception("Unable to Create Directory: " + error_message);
  }
}

void test::Utils::msleep(unsigned int milliseconds) {
#ifdef _WIN32
  Sleep(milliseconds);
#else
  // Convert the milliseconds into a proper timespec structure
  struct timespec requested;
  time_t seconds = static_cast<int>(milliseconds / 1000);
  long int nanoseconds = static_cast<long int>((milliseconds - (seconds * 1000)) * 1000000);

  // Assign the requested time and perform sleep
  requested.tv_sec = seconds;
  requested.tv_nsec = nanoseconds;
  while (nanosleep(&requested, &requested) == -1) {
    continue;
  }
#endif
}

std::string test::Utils::replace_all(const std::string& input, const std::string& from,
                                     const std::string& to) {
  size_t position = 0;
  std::string result = input;
  while ((position = result.find(from, position)) != std::string::npos) {
    result.replace(position, from.length(), to);
    // Handle the case where 'to' is a substring of 'from'
    position += to.length();
  }
  return result;
}

std::string test::Utils::shorten(const std::string& input,
                                 bool add_space_after_newline /*= true*/) {
  std::string result = input;

  // Iterate over each trim delimiter
  std::string delimiters = TRIM_DELIMETERS;
  for (std::string::iterator iterator = delimiters.begin(); iterator < delimiters.end();
       ++iterator) {
    // Replace the trim delimiter with empty string (space if EOL)
    std::string delimiter(1, *iterator);
    std::string newline_replacement = add_space_after_newline ? " " : "";
    std::string replacement = delimiter.compare("\n") == 0 ? newline_replacement : "";
    result = replace_all(result, delimiter, replacement);
  }

  // Return the single line string
  return result;
}

std::string test::Utils::to_lower(const std::string& input) {
  std::string lowercase = input;
  std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);
  return lowercase;
}

std::string test::Utils::trim(const std::string& input) {
  std::string result;
  if (!input.empty()) {
    // Trim right
    result = input.substr(0, input.find_last_not_of(TRIM_DELIMETERS) + 1);
    if (!result.empty()) {
      // Trim left
      result = result.substr(result.find_first_not_of(TRIM_DELIMETERS));
    }
  }
  return result;
}

bool test::Utils::wait_for_port(const std::string& ip_address, unsigned short port,
                                unsigned int number_of_retries /*= 100*/,
                                unsigned int retry_delay_ms /*= 100*/) {
  // Attempt establish a connection to the IP address and port of the node
  for (unsigned int n = 0; n < number_of_retries; ++n) {
    Socket socket;
    try {
      socket.establish_connection(ip_address, port);
      return true;
    } catch (...) {
      msleep(retry_delay_ms);
    }
  }

  // Unable to establish connection to node on port
  return false;
}

std::string test::Utils::home_directory() {
  char home[FILE_PATH_SIZE] = { 0 };
  size_t home_length = sizeof(home);
  uv_os_homedir(home, &home_length);
  return std::string(home, home_length);
}

std::string test::Utils::temp_directory() {
  char tmp[260] = { 0 }; // Note: 260 is the maximum path on Windows
  size_t tmp_length = 260;
  uv_os_tmpdir(tmp, &tmp_length);
  return std::string(tmp, tmp_length);
}
