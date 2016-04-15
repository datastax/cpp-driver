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
#ifndef __DRIVER_OBJECT_OBJECTS_HPP__
#define __DRIVER_OBJECT_OBJECTS_HPP__
#include "cassandra.h"

#include "objects/cluster.hpp"
#include "objects/future.hpp"
#include "objects/iterator.hpp"
#include "objects/prepared.hpp"
#include "objects/result.hpp"
#include "objects/rows.hpp"
#include "objects/session.hpp"
#include "objects/statement.hpp"
#include "objects/uuid_gen.hpp"

#include "smart_ptr.hpp"

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassCollection
     */
    class CollectionDeleter {
    public:
      void operator()(CassCollection* collection) {
        if (collection) {
          cass_collection_free(collection);
        }
      }
    };

    /**
     * Deleter class for driver object CassCustomPayload
     */
    class CustomPayloadDeleter {
    public:
      void operator()(CassCustomPayload* custom_payload) {
        if (custom_payload) {
          cass_custom_payload_free(custom_payload);
        }
      }
    };

    /**
     * Deleter class for driver object CassDataType
     */
    class DataTypeDeleter {
    public:
      void operator()(CassDataType* data_type) {
        if (data_type) {
          cass_data_type_free(data_type);
        }
      }
    };

    /**
     * Deleter class for driver object CassErrorResult
     */
    class ErrorResultDeleter {
    public:
      void operator()(const CassErrorResult* error_result) {
        if (error_result) {
          cass_error_result_free(error_result);
        }
      }
    };

    /**
     * Deleter class for driver object CassRetryPolicy
     */
    class RetryPolicyDeleter {
    public:
      void operator()(CassRetryPolicy* retry_policy) {
        if (retry_policy) {
          cass_retry_policy_free(retry_policy);
        }
      }
    };

    /**
     * Deleter class for driver object CassSchemaMeta
     */
    class SchemaMetaDeleter {
    public:
      void operator()(const CassSchemaMeta* schema_meta) {
        if (schema_meta) {
          cass_schema_meta_free(schema_meta);
        }
      }
    };

    /**
     * Deleter class for driver object CassSsl
     */
    class SslDeleter {
    public:
      void operator()(CassSsl* ssl) {
        if (ssl) {
          cass_ssl_free(ssl);
        }
      }
    };

    /**
     * Deleter class for driver object CassTimestampGen
     */
    class TimestampGenDeleter {
    public:
      void operator()(CassTimestampGen* timestamp_gen) {
        if (timestamp_gen) {
          cass_timestamp_gen_free(timestamp_gen);
        }
      }
    };

    /**
     * Deleter class for driver object CassTuple
     */
    class TupleDeleter {
    public:
      void operator()(CassTuple* tuple) {
        if (tuple) {
          cass_tuple_free(tuple);
        }
      }
    };

    /**
     * Deleter class for driver object CassUserType
     */
    class UserTypeDeleter {
    public:
      void operator()(CassUserType* user_type) {
        if (user_type) {
          cass_user_type_free(user_type);
        }
      }
    };

    // Create scoped pointer types for all driver objects that must be freed
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassCollection, CollectionDeleter> CollectionPtr;
        typedef cass::ScopedPtr<CassCustomPayload, CustomPayloadDeleter> CustomPayloadPtr;
        typedef cass::ScopedPtr<CassDataType, DataTypeDeleter> DataTypePtr;
        typedef cass::ScopedPtr<const CassErrorResult, ErrorResultDeleter> ErrorResultPtr;
        typedef cass::ScopedPtr<CassRetryPolicy, RetryPolicyDeleter> RetryPolicyPtr;
        typedef cass::ScopedPtr<const CassSchemaMeta, SchemaMetaDeleter> SchemaMetaPtr;
        typedef cass::ScopedPtr<CassSsl, SslDeleter> SslPtr;
        typedef cass::ScopedPtr<CassTimestampGen, TimestampGenDeleter> TimestampGenPtr;
        typedef cass::ScopedPtr<CassTuple, TupleDeleter> TuplePtr;
        typedef cass::ScopedPtr<CassUserType, UserTypeDeleter> UserTypePtr;
      }
    }

    // Create shared pointer types for all driver objects that must be freed
    namespace shared {
      namespace native {
        typedef SmartPtr<CassCollection, CollectionDeleter> CollectionPtr;
        typedef SmartPtr<CassCustomPayload, CustomPayloadDeleter> CustomPayloadPtr;
        typedef SmartPtr<CassDataType, DataTypeDeleter> DataTypePtr;
        typedef SmartPtr<const CassErrorResult, ErrorResultDeleter> ErrorResultPtr;
        typedef SmartPtr<CassRetryPolicy, RetryPolicyDeleter> RetryPolicyPtr;
        typedef SmartPtr<const CassSchemaMeta, SchemaMetaDeleter> SchemaMetaPtr;
        typedef SmartPtr<CassSsl, SslDeleter> SslPtr;
        typedef SmartPtr<CassTimestampGen, TimestampGenDeleter> TimestampGenPtr;
        typedef SmartPtr<CassTuple, TupleDeleter> TuplePtr;
        typedef SmartPtr<CassUserType, UserTypeDeleter> UserTypePtr;
      }
    }
  }
}

#endif // __DRIVER_OBJECT_OBJECTS_HPP__
