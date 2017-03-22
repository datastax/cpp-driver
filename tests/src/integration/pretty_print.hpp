/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __PRETTY_PRINT_HPP__
#define __PRETTY_PRINT_HPP__
#include "cassandra.h"

#include <ostream>

/*********************************************************
 * Teach Google test how to print values from the driver *
 ********************************************************/

/**
 * Pretty print CassError values
 *
 * @param error_code CassError value to pretty print
 * @param output_stream Output stream to pretty print value to
 */
void PrintTo(CassError error_code, std::ostream* output_stream);

#endif // __PRETTY_PRINT_HPP__
