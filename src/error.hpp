/*
  Copyright 2014 DataStax

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

#ifndef __CASS_ERROR_HPP_INCLUDED__
#define __CASS_ERROR_HPP_INCLUDED__

#include <string>

#include "cassandra.h"

namespace cass {

struct Error {
  Error(const Error& error) {
    *this = error;
  }

  Error(
      CassErrorSource         source,
      CassError           code,
      const std::string& message,
      const std::string& file,
      int                line) :
      source(source),
      code(code),
      message(message),
      file(file),
      line(line)
  {}

  CassErrorSource  source;
  CassError    code;
  std::string message;
  std::string file;
  int         line;
};

#define CASS_ERROR(source, code, message) \
  new Error(source, code, message, __FILE__, __LINE__)

} // namespace cass

#endif
