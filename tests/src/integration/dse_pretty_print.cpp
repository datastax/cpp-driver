/*
  Copyright (c) 2014-2016 DataStax
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
