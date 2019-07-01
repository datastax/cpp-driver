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

#ifndef __TLOG_HPP__
#define __TLOG_HPP__

#include "options.hpp"

// Create simple console logging functions
#define TEST_LOG_MESSAGE(message, is_output)       \
  if (is_output) {                                 \
    std::cerr << "test> " << message << std::endl; \
  }
#define TEST_LOG(message) TEST_LOG_MESSAGE(message, Options::is_verbose_integration())
#define TEST_LOG_ERROR(message) TEST_LOG_MESSAGE(message, true)

#endif // __TLOG_HPP__
