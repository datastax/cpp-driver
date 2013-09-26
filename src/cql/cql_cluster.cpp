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

#include <boost/bind.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "cql/internal/cql_session_impl.hpp"

#include "cql/cql_connection.hpp"
#include "cql/cql_connection_factory.hpp"

#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"

#include "cql/internal/cql_cluster_impl.hpp"

boost::shared_ptr<cql::cql_cluster_t> 
cql::cql_cluster_t::built_from(const cql_initializer_t& initializer) {
    return boost::shared_ptr<cql::cql_cluster_t>(
		new cql::cql_cluster_t(
			new cql::cql_cluster_pimpl_t(
				initializer.get_contact_points(), 
				initializer.get_configuration())));
}

boost::shared_ptr<cql::cql_builder_t> 
cql::cql_cluster_t::builder() {
    return boost::shared_ptr<cql::cql_builder_t>(
        new cql::cql_builder_t());
}

boost::shared_ptr<cql::cql_session_t> 
cql::cql_cluster_t::connect() {
    return connect("");
}

boost::shared_ptr<cql::cql_session_t> 
cql::cql_cluster_t::connect(const std::string& keyspace) {
    return _pimpl->connect(keyspace);
}

void 
cql::cql_cluster_t::shutdown(int timeout_ms) {
    _pimpl->shutdown(timeout_ms);
}

boost::shared_ptr<cql::cql_metadata_t>
cql::cql_cluster_t::metadata() const {
    return _pimpl->metadata();
}

cql::cql_cluster_t::~cql_cluster_t() {
    shutdown();
}
