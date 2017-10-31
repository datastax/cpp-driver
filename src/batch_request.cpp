/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "batch_request.hpp"

#include <string.h>

extern "C" {

CassError cass_batch_set_execute_as_n(CassBatch* batch,
                                      const char* name, size_t name_length) {
  batch->set_custom_payload("ProxyExecute", reinterpret_cast<const uint8_t *>(name), name_length);
  return CASS_OK;
}

CassError cass_batch_set_execute_as(CassBatch* batch,
                                    const char* name) {
  return cass_batch_set_execute_as_n(batch, name, SAFE_STRLEN(name));
}

} // extern "C"
