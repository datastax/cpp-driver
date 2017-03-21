/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "date_range.hpp"

CassError cass_user_type_set_dse_date_range(CassUserType* user_type,
                                            size_t index,
                                            const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_user_type_set_custom(user_type,
                                   index,
                                   DSE_DATE_RANGE_TYPE,
                                   bytes.data(), bytes.size());
}

CassError cass_user_type_set_dse_date_range_by_name(CassUserType* user_type,
                                                    const char* name,
                                                    const DseDateRange* range) {
  return cass_user_type_set_dse_date_range_by_name_n(user_type,
                                                     name, strlen(name),
                                                     range);
}

CassError cass_user_type_set_dse_date_range_by_name_n(CassUserType* user_type,
                                                      const char* name, size_t name_length,
                                                      const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_user_type_set_custom_by_name_n(user_type,
                                             name, name_length,
                                             DSE_DATE_RANGE_TYPE, sizeof(DSE_DATE_RANGE_TYPE) - 1,
                                             bytes.data(), bytes.size());
}
