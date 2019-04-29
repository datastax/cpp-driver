/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_POINT_HPP_INCLUDED__
#define __DSE_POINT_HPP_INCLUDED__

#include "serialization.hpp"

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

} } } // namespace datastax::internal::enterprise

#endif
