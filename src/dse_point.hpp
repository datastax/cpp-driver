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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_POINT_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_POINT_HPP

#include "dse_serialization.hpp"

namespace datastax { namespace internal { namespace enterprise {

inline Bytes encode_point(cass_double_t x, cass_double_t y) {
  Bytes bytes;

  bytes.reserve(WKB_HEADER_SIZE +       // Header
                sizeof(cass_double_t) + // X
                sizeof(cass_double_t)); // Y

  encode_header_append(WKB_GEOMETRY_TYPE_POINT, bytes);
  encode_append(x, bytes);
  encode_append(y, bytes);

  return bytes;
}

}}} // namespace datastax::internal::enterprise

#endif
