/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_CASSANDRA_HPP_INCLUDED__
#define __CASS_CASSANDRA_HPP_INCLUDED__

#include "cassandra.h"
#include "cluster.hpp"
#include "session.hpp"
#include "statement.hpp"
#include "future.hpp"
#include "prepared.hpp"
#include "batch_request.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "value.hpp"

// This abstraction allows us to separate internal types from the
// external opaque pointers that we expose.
template <typename In, typename Ex>
struct External : public In {
  In* from() { return static_cast<In*>(this); }
  const In* from() const { return static_cast<const In*>(this); }
  static Ex* to(In* in) { return static_cast<Ex*>(in); }
  static const Ex* to(const In* in) {
    return static_cast<const Ex*>(in);
  }
};

extern "C" {

struct CassCluster_ : public External<cass::Cluster, CassCluster> {};
struct CassSession_ : public External<cass::Session, CassSession> {};
struct CassStatement_ : public External<cass::Statement, CassStatement> {};
struct CassFuture_ : public External<cass::Future, CassFuture> {};
struct CassPrepared_ : public External<cass::Prepared, CassPrepared> {};
struct CassBatch_ : public External<cass::BatchRequest, CassBatch> {};
struct CassResult_ : public External<cass::ResultResponse, CassResult> {};
struct CassCollection_ : public External<cass::Collection, CassCollection> {};
struct CassIterator_ : public External<cass::Iterator, CassIterator> {};
struct CassRow_ : public External<cass::Row, CassRow> {};
struct CassValue_ : public External<cass::Value, CassValue> {};
}

#endif
