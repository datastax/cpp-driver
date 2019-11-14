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

#include "dse_pretty_print.hpp"

void PrintTo(DseGraphResultType type, std::ostream* output_stream) {
  switch (type) {
    case DSE_GRAPH_RESULT_TYPE_NULL:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_NULL";
      break;
    case DSE_GRAPH_RESULT_TYPE_BOOL:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_BOOL";
      break;
    case DSE_GRAPH_RESULT_TYPE_NUMBER:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_NUMBER";
      break;
    case DSE_GRAPH_RESULT_TYPE_STRING:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_STRING";
      break;
    case DSE_GRAPH_RESULT_TYPE_OBJECT:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_OBJECT";
      break;
    case DSE_GRAPH_RESULT_TYPE_ARRAY:
      *output_stream << "DSE_GRAPH_RESULT_TYPE_ARRAY";
      break;
    default:
      *output_stream << "DseGraphResultType NEEDS TO BE ADDED";
  }
}
