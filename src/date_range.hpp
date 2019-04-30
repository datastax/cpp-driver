/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_ENTERPRISE_INTERNAL_DATE_RANGE_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_DATE_RANGE_HPP

#include "serialization.hpp"

namespace datastax { namespace internal { namespace enterprise {

  Bytes encode_date_range(const DseDateRange *range);

} } } // datastax::internal::enterprise

#endif
