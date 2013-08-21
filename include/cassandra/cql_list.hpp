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

#ifndef CQL_LIST_H_
#define CQL_LIST_H_

#include "cassandra/cql.hpp"

namespace cql {

    class cql_list_t
    {

    public:
        virtual
        ~cql_list_t(){};

        virtual std::string
        str() const = 0;

        virtual cql::cql_column_type_enum
        element_type() const = 0;

        virtual const std::string&
        custom_class() const = 0;

        virtual bool
        get_bool(size_t i,
                 bool& output) const = 0;

        virtual bool
        get_int(size_t i,
                cql_int_t& output) const = 0;

        virtual bool
        get_float(size_t i,
                  float& output) const = 0;

        virtual bool
        get_double(size_t i,
                   double& output) const = 0;

        virtual bool
        get_bigint(size_t i,
                   cql::cql_bigint_t& output) const = 0;

        virtual bool
        get_string(size_t i,
                   std::string& output) const = 0;

        virtual bool
        get_data(size_t i,
                 cql::cql_byte_t** output,
                 cql::cql_short_t& size) const = 0;

        virtual size_t
        size() const = 0;

    };

} // namespace cql

#endif // CQL_LIST_H_
