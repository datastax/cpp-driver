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

#include "driver_info.hpp"

#include "cassandra.h"
#include "macros.hpp"

#include <cstring>

#define DRIVER_VERSION \
  STRINGIFY(CASS_VERSION_MAJOR) "." STRINGIFY(CASS_VERSION_MINOR) "." STRINGIFY(CASS_VERSION_PATCH)

namespace datastax { namespace internal {

const char* driver_name() {
  return "DataStax C/C++ Driver for Apache Cassandra and DataStax Products";
}

const char* driver_version() {
  if (strlen(CASS_VERSION_SUFFIX) == 0) {
    return DRIVER_VERSION;
  } else {
    return DRIVER_VERSION "-" CASS_VERSION_SUFFIX;
  }
}

}} // namespace datastax::internal
