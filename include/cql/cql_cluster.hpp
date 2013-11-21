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

#ifndef CQL_CLUSTER_H_
#define CQL_CLUSTER_H_

#include <memory>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

#include <cql/cql_config.hpp>

namespace cql {

class cql_session_t;
class cql_initializer_t;
class cql_builder_t;
class cql_metadata_t;


class CQL_EXPORT cql_cluster_t: boost::noncopyable {
public:
    static boost::shared_ptr<cql_cluster_t>
    built_from(cql_initializer_t& initializer);

    static boost::shared_ptr<cql_builder_t>
    builder();

    virtual boost::shared_ptr<cql_session_t>
    connect() = 0;

    virtual boost::shared_ptr<cql_session_t>
    connect(const std::string& keyspace) = 0;

    virtual void
    shutdown(int timeout_ms = -1) = 0;

    // returns MOCK metadata object.
    virtual boost::shared_ptr<cql_metadata_t>
    metadata() const = 0;

    virtual inline
    ~cql_cluster_t() { }
};

}

#endif // CQL_CLUSTER_H_
