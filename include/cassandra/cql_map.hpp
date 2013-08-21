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

#ifndef CQL_MAP_H_
#define CQL_MAP_H_

#include "cassandra/cql.hpp"

namespace cql {

    class cql_map_t
    {

    public:
        virtual
        ~cql_map_t(){};

        virtual bool
        get_key_bool(size_t i,
                     bool& output) const = 0;

        virtual bool
        get_key_int(size_t i,
                    cql_int_t& output) const = 0;

        virtual bool
        get_key_float(size_t i,
                      float& output) const = 0;

        virtual bool
        get_key_double(size_t i,
                       double& output) const = 0;

        virtual bool
        get_key_bigint(size_t i,
                       cql::cql_bigint_t& output) const = 0;

        virtual bool
        get_key_string(size_t i,
                       std::string& output) const = 0;

        virtual bool
        get_key_data(size_t i,
                     cql::cql_byte_t** output,
                     cql::cql_short_t& size) const = 0;

        virtual bool
        get_value_bool(size_t i,
                       bool& output) const = 0;

        virtual bool
        get_value_int(size_t i,
                      cql_int_t& output) const = 0;

        virtual bool
        get_value_float(size_t i,
                        float& output) const = 0;

        virtual bool
        get_value_double(size_t i,
                         double& output) const = 0;

        virtual bool
        get_value_bigint(size_t i,
                         cql::cql_bigint_t& output) const = 0;

        virtual bool
        get_value_string(size_t i,
                         std::string& output) const = 0;

        virtual bool
        get_value_data(size_t i,
                       cql::cql_byte_t** output,
                       cql::cql_short_t& size) const = 0;

        virtual std::string
        str() const = 0;

        virtual cql::cql_column_type_enum
        key_type() const = 0;

        virtual const std::string&
        key_custom_class() const = 0;

        virtual cql::cql_column_type_enum
        value_type() const = 0;

        virtual const std::string&
        value_custom_class() const = 0;

        virtual size_t
        size() const = 0;

    };


} // namespace cql

#endif // CQL_MAP_H_
