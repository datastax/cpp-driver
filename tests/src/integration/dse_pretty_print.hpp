/*
  Copyright (c) 2014-2016 DataStax
*/
#ifndef __DSE_PRETTY_PRINT_HPP__
#define __DSE_PRETTY_PRINT_HPP__
#include "dse.h"
#include "pretty_print.hpp"

#include <ostream>

/*********************************************************
 * Teach Google test how to print values from the driver *
 ********************************************************/

/**
 * Pretty print DseGraphResultType values
 *
 * @param type DseGraphResultType value to pretty print
 * @param output_stream Output stream to pretty print value to
 */
void PrintTo(DseGraphResultType type, std::ostream* output_stream);

#endif // __PRETTY_PRINT_HPP__
