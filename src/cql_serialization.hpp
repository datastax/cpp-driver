/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#ifndef CQL_SERIALIZATION_H_
#define CQL_SERIALIZATION_H_

#include <istream>
#include <list>
#include <map>
#include <ostream>
#include <string>
#include <vector>

#include "cql/cql.hpp"

namespace cql {

std::ostream&
encode_bool(std::ostream& output,
            bool value);

void
encode_bool(std::vector<cql::cql_byte_t>& output,
            const bool value);

std::istream&
decode_bool(std::istream& input,
            bool& value);

bool
decode_bool(const std::vector<cql::cql_byte_t>& input);

std::ostream&
encode_short(std::ostream& output,
             cql::cql_short_t value);

void
encode_short(std::vector<cql::cql_byte_t>& output,
             const cql::cql_short_t value);

std::istream&
decode_short(std::istream& input,
             cql::cql_short_t& value);

cql::cql_short_t
decode_short(const std::vector<cql::cql_byte_t>& input);

cql::cql_byte_t*
decode_short(cql::cql_byte_t*,
             cql::cql_short_t& value);

std::ostream&
encode_int(std::ostream& output,
           const cql::cql_int_t value);

void
encode_int(std::vector<cql::cql_byte_t>& output,
           const cql::cql_int_t value);

std::istream&
decode_int(std::istream& input,
           cql::cql_int_t& value);

cql::cql_byte_t*
decode_int(cql::cql_byte_t* input,
           cql::cql_int_t& output);

cql_int_t
decode_int(const std::vector<cql::cql_byte_t>& input);


std::ostream&
encode_float(std::ostream& output,
             const float value);

void
encode_float(std::vector<cql::cql_byte_t>& output,
             const float value);

std::istream&
decode_float(std::istream& input,
             float& value);

float
decode_float(const std::vector<cql::cql_byte_t>& input);

cql::cql_byte_t*
decode_float(cql::cql_byte_t* input,
             float& output);

std::ostream&
encode_double(std::ostream& output,
              const double value);
void
encode_double(std::vector<cql::cql_byte_t>& output,
              const double value);

std::istream&
decode_double(std::istream& input,
              double& value);

double
decode_double(const std::vector<cql::cql_byte_t>& input);


std::istream&
decode_double(std::istream& input,
              double& value);

cql::cql_byte_t*
decode_double(cql::cql_byte_t* input,
              double& output);

std::ostream&
encode_bigint(std::ostream& output,
              const cql::cql_bigint_t value);

void
encode_bigint(std::vector<cql::cql_byte_t>& output,
              const cql::cql_bigint_t value);

std::istream&
decode_bigint(std::istream& input,
              cql::cql_bigint_t& value);

cql::cql_bigint_t
decode_bigint(const std::vector<cql::cql_byte_t>& input);

cql::cql_byte_t*
decode_bigint(cql::cql_byte_t* input,
              cql::cql_bigint_t& output);

std::ostream&
encode_string(std::ostream& output,
              const std::string& value);

std::istream&
decode_string(std::istream& input,
              std::string& value);

std::string
decode_string(const std::vector<cql::cql_byte_t>& input);

cql::cql_byte_t*
decode_string(cql::cql_byte_t* input,
              std::string& value);

std::ostream&
encode_bytes(std::ostream& output,
             const std::vector<cql::cql_byte_t>& value);

std::istream&
decode_bytes(std::istream& input,
             std::vector<cql::cql_byte_t>& value);

std::ostream&
encode_short_bytes(std::ostream& output,
                   const std::vector<cql::cql_byte_t>& value);

std::istream&
decode_short_bytes(std::istream& input,
                   std::vector<cql::cql_byte_t>& value);

cql::cql_byte_t*
decode_short_bytes(cql::cql_byte_t* input,
                   std::vector<cql::cql_byte_t>& value);

std::ostream&
encode_long_string(std::ostream& output,
                   const std::string& value);

std::istream&
decode_long_string(std::istream& input,
                   std::string& value);

std::ostream&
encode_string_list(std::ostream& output,
                   const std::list<std::string>& list);

std::istream&
decode_string_list(std::istream& input,
                   std::list<std::string>& list);

std::ostream&
encode_string_map(std::ostream& output,
                  const std::map<std::string, std::string>& map);

std::istream&
decode_string_map(std::istream& input,
                  std::map<std::string, std::string>& map);

std::ostream&
encode_string_multimap(std::ostream& output,
                       const std::map<std::string, std::list<std::string> >& map);

std::istream&
decode_string_multimap(std::istream& input,
                       std::map<std::string, std::list<std::string> >& map);

std::ostream&
encode_option(std::ostream& output,
              cql::cql_column_type_enum& id,
              const std::string& value);

std::istream&
decode_option(std::istream& input,
              cql::cql_column_type_enum& id,
              std::string& value);

cql::cql_byte_t*
decode_option(cql::cql_byte_t* input,
              cql::cql_column_type_enum& id,
              std::string& value);

std::string
decode_ipv4_from_bytes(const cql::cql_byte_t* data);
    
std::string
decode_ipv6_from_bytes(const cql::cql_byte_t* data);

std::ostream&
encode_ipv4(std::ostream& output,
            const std::string& ip);

std::ostream&
encode_ipv6(std::ostream& output,
            const std::string& ip);

std::ostream&
encode_inet(std::ostream& output,
            const std::string& ip,
            const cql_int_t port);

std::istream&
decode_inet(std::istream& input,
            std::string& ip,
            cql_int_t& port);

} // namespace cql

#endif // CQL_SERIALIZATION_H_
