/*
  Copyright 2014 DataStax

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

#ifndef __CASS_COLUMN_METADATA_HPP_INCLUDED__
#define __CASS_COLUMN_METADATA_HPP_INCLUDED__

#include "cassandra.h"

#include "third_party/boost/boost/cstdint.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace cass {

struct ColumnMetadata {
  ColumnMetadata()
      : type(CASS_VALUE_TYPE_UNKNOWN)
      , keyspace(NULL)
      , keyspace_size(0)
      , table(NULL)
      , table_size(0)
      , name(NULL)
      , name_size(0)
      , class_name(NULL)
      , class_name_size(0)
      , collection_primary_type(CASS_VALUE_TYPE_UNKNOWN)
      , collection_primary_class(NULL)
      , collection_primary_class_size(0)
      , collection_secondary_type(CASS_VALUE_TYPE_UNKNOWN)
      , collection_secondary_class(NULL)
      , collection_secondary_class_size(0) {}

  uint16_t type;
  char* keyspace;
  size_t keyspace_size;

  char* table;
  size_t table_size;

  char* name;
  size_t name_size;

  char* class_name;
  size_t class_name_size;

  uint16_t collection_primary_type;
  char* collection_primary_class;

  size_t collection_primary_class_size;
  uint16_t collection_secondary_type;

  char* collection_secondary_class;
  size_t collection_secondary_class_size;
};

struct CaseInsensitiveLessThan {
  struct CaseInsensitiveCharLessThan {
    bool operator() (int c1, int c2) {
      return tolower(c1) < tolower(c2);
    }
  };
  bool operator() (const std::string& s1, const std::string& s2) const {
    return std::lexicographical_compare(s1.begin(), s1.end(),
                                        s2.begin(), s2.end(),
                                        CaseInsensitiveCharLessThan());
  }
};

typedef std::vector<ColumnMetadata> ColumnMetadataVec;
typedef std::vector<size_t> ColumnIndexVec;
typedef std::map<std::string, ColumnIndexVec, CaseInsensitiveLessThan> ColumnMetadataIndex;

} // namespace cass

#endif
