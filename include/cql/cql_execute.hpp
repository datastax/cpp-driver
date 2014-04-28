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

#ifndef CQL_EXECUTE_H_
#define CQL_EXECUTE_H_

#include <vector>
#include <boost/noncopyable.hpp>
#include <boost/make_shared.hpp>

#include "cql/cql_config.hpp"
#include "cql/cql.hpp"

#include "cql/policies/cql_default_retry_policy.hpp"

namespace cql {

class cql_message_execute_impl_t;

class CQL_EXPORT cql_execute_t :
        boost::noncopyable {

public:

    cql_execute_t();

    ~cql_execute_t();

    cql_execute_t(const std::vector<cql::cql_byte_t>& id,
                  cql::cql_consistency_enum consistency,
                  boost::shared_ptr<cql_retry_policy_t> retry_policy = boost::shared_ptr<cql_retry_policy_t>(new cql_default_retry_policy_t()));

    const std::vector<cql::cql_byte_t>&
    query_id() const;

    void
    query_id(const std::vector<cql::cql_byte_t>& id);

    cql::cql_consistency_enum
    consistency() const;

    void
    consistency(const cql::cql_consistency_enum consistency);

    void
    push_back(const std::vector<cql::cql_byte_t>& val);

    void
    push_back(const std::string& val);
            
    void
    push_back(const char* val);

    void
    push_back(const cql::cql_short_t val);

    void
    push_back(const cql_int_t val);

    void
    push_back(const cql::cql_bigint_t val);

    void
    push_back(const float val);

    void
    push_back(const double val);

    void
    push_back(const bool val);
    
    void
    skip();

    void
    pop_back();

    boost::shared_ptr<cql_message_execute_impl_t>
    impl() const;
            
    boost::shared_ptr<cql_retry_policy_t>
    retry_policy() const;
            
    void
    set_retry_policy(
        const boost::shared_ptr<cql_retry_policy_t>& retry_policy);
            
    bool
    has_retry_policy() const;
            
    void
    increment_retry_counter();
            
    int
    get_retry_counter() const;

    cql_stream_t
    stream();
            
    void
    set_stream(const cql_stream_t& stream);
     
    std::string 
    str() const;

private:
    boost::shared_ptr<cql_message_execute_impl_t> _impl;
};

} // namespace cql

#endif // CQL_EXECUTE_H_
