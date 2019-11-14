/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef DATASTAX_ENTERPRISE_INTERNAL_VALIDATE_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_VALIDATE_HPP

#include "dse.h"

#include "string_ref.hpp"

namespace datastax { namespace internal { namespace enterprise {

inline CassError validate_data_type(const CassValue* value, const char* class_name) {
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

  if (StringRef(name, name_length) != class_name) {
    return CASS_ERROR_LIB_INVALID_CUSTOM_TYPE;
  }

  return CASS_OK;
}

}}} // namespace datastax::internal::enterprise

#endif
