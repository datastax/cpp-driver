/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_VALUE_HPP_INCLUDED__
#define __CASS_VALUE_HPP_INCLUDED__

#include "cassandra.h"

#include "buffer_piece.hpp"

namespace cass {

struct Value {
  Value()
      : type(CASS_VALUE_TYPE_UNKNOWN)
      , primary_type(CASS_VALUE_TYPE_UNKNOWN)
      , secondary_type(CASS_VALUE_TYPE_UNKNOWN)
      , count(0) {}

  Value(CassValueType type, char* data, size_t size)
      : type(type)
      , primary_type(CASS_VALUE_TYPE_UNKNOWN)
      , secondary_type(CASS_VALUE_TYPE_UNKNOWN)
      , count(0)
      , buffer(data, size) {}

  CassValueType type;
  CassValueType primary_type;
  CassValueType secondary_type;
  uint16_t count;
  BufferPiece buffer;
};

} // namespace cass

#endif
