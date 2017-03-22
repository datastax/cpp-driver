/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_DATE_RANGE_HPP_INCLUDED__
#define __DSE_DATE_RANGE_HPP_INCLUDED__

#include "serialization.hpp"

namespace dse {
  Bytes encode_date_range(const DseDateRange *range);
}

#endif