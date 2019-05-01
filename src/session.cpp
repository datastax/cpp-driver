/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"

#include "session.hpp"

extern "C" {

CassUuid cass_session_get_client_id(CassSession* session) { return session->client_id(); }

} // extern "C"
