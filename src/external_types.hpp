/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_EXTERNAL_TYPES_HPP_INCLUDED__
#define __CASS_EXTERNAL_TYPES_HPP_INCLUDED__

#include "auth.hpp"
#include "cassandra.h"
#include "batch_request.hpp"
#include "cluster.hpp"
#include "collection.hpp"
#include "data_type.hpp"
#include "error_response.hpp"
#include "future.hpp"
#include "iterator.hpp"
#include "metadata.hpp"
#include "prepared.hpp"
#include "result_response.hpp"
#include "request.hpp"
#include "retry_policy.hpp"
#include "row.hpp"
#include "session.hpp"
#include "ssl.hpp"
#include "statement.hpp"
#include "timestamp_generator.hpp"
#include "tuple.hpp"
#include "user_type_value.hpp"
#include "uuids.hpp"
#include "value.hpp"

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
EXTERNAL_TYPE(cass::ErrorResponse, CassErrorResult);
EXTERNAL_TYPE(cass::Collection, CassCollection);
EXTERNAL_TYPE(cass::Iterator, CassIterator);
EXTERNAL_TYPE(cass::Row, CassRow);
EXTERNAL_TYPE(cass::Value, CassValue);
EXTERNAL_TYPE(cass::SslContext, CassSsl);
EXTERNAL_TYPE(cass::ExternalAuthenticator, CassAuthenticator);
EXTERNAL_TYPE(cass::Metadata::SchemaSnapshot, CassSchemaMeta);
EXTERNAL_TYPE(cass::KeyspaceMetadata, CassKeyspaceMeta);
EXTERNAL_TYPE(cass::TableMetadata, CassTableMeta);
EXTERNAL_TYPE(cass::ViewMetadata, CassMaterializedViewMeta);
EXTERNAL_TYPE(cass::ColumnMetadata, CassColumnMeta);
EXTERNAL_TYPE(cass::IndexMetadata, CassIndexMeta);
EXTERNAL_TYPE(cass::FunctionMetadata, CassFunctionMeta);
EXTERNAL_TYPE(cass::AggregateMetadata, CassAggregateMeta);
EXTERNAL_TYPE(cass::UuidGen, CassUuidGen);
EXTERNAL_TYPE(cass::Tuple, CassTuple);
EXTERNAL_TYPE(cass::UserTypeValue, CassUserType);
EXTERNAL_TYPE(cass::DataType, CassDataType);
EXTERNAL_TYPE(cass::TimestampGenerator, CassTimestampGen);
EXTERNAL_TYPE(cass::RetryPolicy, CassRetryPolicy);
EXTERNAL_TYPE(cass::CustomPayload, CassCustomPayload);

}

#undef EXTERNAL_TYPE

#endif
