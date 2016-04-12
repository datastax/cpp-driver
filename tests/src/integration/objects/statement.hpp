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
#ifndef __DRIVER_OBJECT_STATEMENT_HPP__
#define __DRIVER_OBJECT_STATEMENT_HPP__
#include "cassandra.h"

#include "objects/future.hpp"

#include "smart_ptr.hpp"

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassBatch
     */
    class BatchDeleter {
    public:
      void operator()(CassBatch* batch) {
        if (batch) {
          cass_batch_free(batch);
        }
      }
    };

    /**
     * Deleter class for driver object CassStatement
     */
    class StatementDeleter {
    public:
      void operator()(CassStatement* statement) {
        if (statement) {
          cass_statement_free(statement);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassBatch, BatchDeleter> BatchPtr;
        typedef cass::ScopedPtr<CassStatement, StatementDeleter> StatementPtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<CassBatch, BatchDeleter> BatchPtr;
        typedef SmartPtr<CassStatement, StatementDeleter> StatementPtr;
      }
    }

    /**
     * Wrapped statement object
     */
    class Statement {
    friend class Batch;
    friend class Session;
    public:
      /**
       * Create the statement object from the native driver statement object
       *
       * @param statement Native driver object
       */
      Statement(CassStatement* statement)
        : statement_(statement) {}

      /**
       * Create the statement object from the shared reference
       *
       * @param statement Shared reference
       */
      Statement(shared::native::StatementPtr statement)
        : statement_(statement) {}

      /**
       * Create the statement object from a query
       *
       * @param query Query to create statement from
       * @param parameter_count Number of parameters contained in the query
       */
      Statement(const std::string& query, size_t parameter_count) {
        statement_ = cass_statement_new(query.c_str(), parameter_count);
      }

      /**
       * Bind a value to the statement
       *
       * @param index Index to bind the value to
       * @param value<T> Value to bind to the statement at given index
       */
      template<typename T>
      void bind(size_t index, T value) {
        value.statement_bind(statement_, index);
      }

    private:
      /**
       * Statement driver reference object
       */
      shared::native::StatementPtr statement_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Statement> StatementPtr;
    }
    namespace shared {
      typedef SmartPtr<Statement> StatementPtr;
    }

    /**
     * Wrapped batch object
     */
    class Batch {
    friend class Session;
    public:
      /**
       * Create the batch object from the native driver statement object
       *
       * @param batch_type Type of batch to create (default: Unlogged)
       */
      Batch(CassBatchType batch_type = CASS_BATCH_TYPE_UNLOGGED)
        : batch_(cass_batch_new(batch_type)) {}

      /**
       * Create the batch object from the native driver statement object
       *
       * @param batch Native driver object
       */
      Batch(CassBatch* batch)
        : batch_(batch) {}

      /**
       * Create the batch object from the shared reference
       *
       * @param batch Shared reference
       */
      Batch(shared::native::BatchPtr batch)
        : batch_(batch) {}

      /**
       * Add a statement (query or bound) to the batch
       *
       * @param statement Query or bound statement to add
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       */
      void add(shared::StatementPtr statement, bool assert_ok = true) {
        CassError error_code = cass_batch_add_statement(batch_.get(),
          statement->statement_.get());
        if (assert_ok) {
          ASSERT_EQ(CASS_OK, error_code)
            << "Unable to Add Statement to Batch: " << cass_error_desc(error_code);
        }

      }

    private:
      /**
       * Batch driver reference object
       */
      shared::native::BatchPtr batch_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Batch> BatchPtr;
    }
    namespace shared {
      typedef SmartPtr<Batch> BatchPtr;
    }
  }
}

#endif // __DRIVER_OBJECT_STATEMENT_HPP__
