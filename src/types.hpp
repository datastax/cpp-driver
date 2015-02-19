/*
  Copyright (c) 2014-2015 DataStax

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
#include "schema_metadata.hpp"
#include "session.hpp"
#include "statement.hpp"
#include "future.hpp"
#include "prepared.hpp"
#include "batch_request.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "value.hpp"
#include "iterator.hpp"
#include "ssl.hpp"
#include "uuids.hpp"

// This abstraction allows us to separate internal types from the
// external opaque pointers that we expose.
template <typename In, typename Ex>
struct External : public In {
  In* from() { return static_cast<In*>(this); }
  const In* from() const { return static_cast<const In*>(this); }
  static Ex* to(In* in) { return static_cast<Ex*>(in); }
  static const Ex* to(const In* in) { return static_cast<const Ex*>(in); }
};

#define EXTERNAL_TYPE(InternalType, ExternalType)                        \
  struct ExternalType##_ : public External<InternalType, ExternalType> { \
    private:                                                             \
      ~ExternalType##_() { }                                             \
  }

extern "C" {

EXTERNAL_TYPE(cass::Cluster, CassCluster);
EXTERNAL_TYPE(cass::Session, CassSession);
EXTERNAL_TYPE(cass::Statement, CassStatement);
EXTERNAL_TYPE(cass::Future, CassFuture);
EXTERNAL_TYPE(cass::Prepared, CassPrepared);
EXTERNAL_TYPE(cass::BatchRequest, CassBatch);
EXTERNAL_TYPE(cass::ResultResponse, CassResult);
EXTERNAL_TYPE(cass::BufferCollection, CassCollection);
EXTERNAL_TYPE(cass::Iterator, CassIterator);
EXTERNAL_TYPE(cass::Row, CassRow);
EXTERNAL_TYPE(cass::Value, CassValue);
EXTERNAL_TYPE(cass::SslContext, CassSsl);
EXTERNAL_TYPE(cass::Schema, CassSchema);
EXTERNAL_TYPE(cass::SchemaMetadata, CassSchemaMeta);
EXTERNAL_TYPE(cass::SchemaMetadataField, CassSchemaMetaField);
EXTERNAL_TYPE(cass::UuidGen, CassUuidGen);

}

#undef EXTERNAL_TYPE

#endif
