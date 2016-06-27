/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_VALIDATE_HPP_INCLUDED__
#define __DSE_VALIDATE_HPP_INCLUDED__

#include "dse.h"

#include "string_ref.hpp"

namespace dse {

inline CassError validate_data_type(const CassValue* value,
                               const char* class_name) {
  const CassDataType* data_type = cass_value_data_type(value);

  if (data_type == NULL) {
    return CASS_ERROR_LIB_INTERNAL_ERROR;
  }

  if (cass_data_type_type(data_type) != CASS_VALUE_TYPE_CUSTOM) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const char* name;
  size_t name_length;
  cass_data_type_class_name(data_type, &name, &name_length);

  if (cass::StringRef(name, name_length) != class_name) {
    return CASS_ERROR_LIB_INVALID_CUSTOM_TYPE;
  }

  return CASS_OK;
}

} // namespace dse


#endif
