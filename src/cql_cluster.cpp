/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <boost/bind.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "cql/cql_connection.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql/internal/cql_cluster_impl.hpp"
#include "cql/internal/cql_connection_factory.hpp"
#include "cql/internal/cql_session_impl.hpp"

boost::shared_ptr<cql::cql_cluster_t>
cql::cql_cluster_t::built_from(
    cql_initializer_t& initializer)
{
    return boost::shared_ptr<cql::cql_cluster_t>(
        new cql::cql_cluster_impl_t(
            initializer.contact_points(),
            initializer.configuration()));
}

boost::shared_ptr<cql::cql_builder_t>
cql::cql_cluster_t::builder()
{
    return boost::shared_ptr<cql::cql_builder_t>(
        new cql::cql_builder_t());
}
