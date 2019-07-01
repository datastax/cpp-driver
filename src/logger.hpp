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

#ifndef DATASTAX_INTERNAL_LOGGER_HPP
#define DATASTAX_INTERNAL_LOGGER_HPP

#include "cassandra.h"
#include "get_time.hpp"
#include "string.hpp"

#include <cstring>
#include <stdarg.h>
#include <stdio.h>

namespace datastax { namespace internal {

class Logger {
public:
  static void set_log_level(CassLogLevel level);
  static void set_callback(CassLogCallback cb, void* data);

#if defined(__GNUC__) || defined(__clang__)
#define ATTR_FORMAT(string, first) __attribute__((__format__(__printf__, string, first)))
#else
#define ATTR_FORMAT(string, first)
#endif

  static CassLogLevel log_level() { return log_level_; }

  ATTR_FORMAT(5, 6)
  static void log(CassLogLevel severity, const char* file, int line, const char* function,
                  const char* format, ...) {
    va_list args;
    va_start(args, format);
    internal_log(severity, file, line, function, format, args);
    va_end(args);
  }

private:
  ATTR_FORMAT(5, 0)
  static void internal_log(CassLogLevel severity, const char* file, int line, const char* function,
                           const char* format, va_list args);

private:
  static CassLogLevel log_level_;
  static CassLogCallback cb_;
  static void* data_;

  Logger(); // Keep this object from being created
};

}} // namespace datastax::internal

// These macros allow the LOG_<level>() methods to accept one or more
// arguments (including the format string). This needs to be extended
// if using greater than 20 arguments. The first macro alway returns
// the first argument. The second macro returns the rest of the arguments
// if the number of variadic arguments is greater than one.
#define LOG_FIRST_(...) LOG_FIRST_HELPER_(__VA_ARGS__, throwaway)
#define LOG_FIRST_HELPER_(format, ...) format
#define LOG_REST_(...) LOG_REST_HELPER_(LOG_NUM_ARGS_(__VA_ARGS__), __VA_ARGS__)
#define LOG_REST_HELPER_(num, ...) LOG_REST_HELPER2_(num, __VA_ARGS__)
#define LOG_REST_HELPER2_(num, ...) LOG_REST_HELPER_##num##_(__VA_ARGS__)
#define LOG_REST_HELPER_ONE_(first)
#define LOG_REST_HELPER_TWOORMORE_(first, ...) , __VA_ARGS__
#define LOG_SELECT_20TH_(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, \
                         a17, a18, a19, a20, ...)                                               \
  a20
#define LOG_NUM_ARGS_(...)                                                                        \
  LOG_SELECT_20TH_(__VA_ARGS__, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, \
                   TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE,   \
                   TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, TWOORMORE, ONE, throwaway)

#ifdef WIN32
#define LOG_FILE_SEPARATOR_STR_ "\\"
#define LOG_FILE_SEPARATOR_CHAR_ '\\'
#else
#define LOG_FILE_SEPARATOR_STR_ "/"
#define LOG_FILE_SEPARATOR_CHAR_ '/'
#endif

// Compiles away on most compilers (not MSVC, but it's still fast). This adds the seperator to the
// front to guarantee a match is found, but it's then removed by moving the pointer to the next
// position.
#define LOG_FILE_ (strrchr(LOG_FILE_SEPARATOR_STR_ __FILE__, LOG_FILE_SEPARATOR_CHAR_) + 1)

#if defined(_MSC_VER)
#define LOG_FUNCTION_ __FUNCSIG__
#elif defined(__clang__) || defined(__GNUC__) || defined(__INTEL_COMPILER)
#define LOG_FUNCTION_ __PRETTY_FUNCTION__
#elif defined(__func__)
#define LOG_FUNCTION_ __func__
#else
#define LOG_FUNCTION_ ""
#endif

#define LOG_CHECK_LEVEL(severity, ...)                                                   \
  do {                                                                                   \
    if (severity <= ::datastax::internal::Logger::log_level()) {                         \
      ::datastax::internal::Logger::log(severity, LOG_FILE_, __LINE__, LOG_FUNCTION_,    \
                                        LOG_FIRST_(__VA_ARGS__) LOG_REST_(__VA_ARGS__)); \
    }                                                                                    \
  } while (0)

#define LOG_CRITICAL(...) LOG_CHECK_LEVEL(CASS_LOG_CRITICAL, __VA_ARGS__)
#define LOG_ERROR(...) LOG_CHECK_LEVEL(CASS_LOG_ERROR, __VA_ARGS__)
#define LOG_WARN(...) LOG_CHECK_LEVEL(CASS_LOG_WARN, __VA_ARGS__)
#define LOG_INFO(...) LOG_CHECK_LEVEL(CASS_LOG_INFO, __VA_ARGS__)
#define LOG_DEBUG(...) LOG_CHECK_LEVEL(CASS_LOG_DEBUG, __VA_ARGS__)
#define LOG_TRACE(...) LOG_CHECK_LEVEL(CASS_LOG_TRACE, __VA_ARGS__)

#endif
