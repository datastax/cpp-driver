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
#ifndef __DRIVER_OBJECT_SESSION_HPP__
#define __DRIVER_OBJECT_SESSION_HPP__
#include "cassandra.h"

#include "objects/exception.hpp"

#include "objects/future.hpp"
#include "objects/prepared.hpp"
#include "objects/result.hpp"
#include "objects/statement.hpp"

#include "smart_ptr.hpp"

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassSession
     */
    class SessionDeleter {
    public:
      void operator()(CassSession* session) {
        if (session) {
          cass_session_free(session);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassSession, SessionDeleter> SessionPtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<CassSession, SessionDeleter> SessionPtr;
      }
    }

    /**
     * Wrapped session object
     */
    class Session {
    friend class Cluster; 
    public:
      class Exception : public driver::object::Exception {
      public:
        Exception(const std::string& message, const CassError error_code)
          : driver::object::Exception(message, error_code) {}
        ~Exception() throw() {}
      };

      /**
       * Create the session object from the native driver object
       *
       * @param session Native driver object
       */
      Session(CassSession* session)
        : session_(session) {}

      /**
       * Create the session object from a shared reference
       *
       * @param session Shared reference
       */
      Session(shared::native::SessionPtr session)
        : session_(session) {}

      /**
       * Close the active session
       */
      void close() {
        Future close_future = cass_session_close(session_.get());
        close_future.wait();
      }

      /**
       * Execute a batch statement synchronously
       *
       * @param batch Batch statement to execute
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       */
      void execute(Batch batch, bool assert_ok = true) {
        Future future = cass_session_execute_batch(session_.get(),
          batch.batch_.get());
        future.wait(assert_ok);
      }

      /**
       * Execute a statement synchronously
       *
       * @param statement Query or bound statement to execute
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       * @return Result object
       */
      shared::ResultPtr execute(shared::StatementPtr statement, bool assert_ok = true) {
        shared::FuturePtr future = new Future(cass_session_execute(session_.get(),
          statement->statement_.get()));
        future->wait(assert_ok);
        return new Result(future);
      }

      /**
       * Execute a query synchronously
       *
       * @param query Query to execute as a simple statement
       * @param consistency Consistency level to execute the query at
       *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       * @return Result object
       */
      shared::ResultPtr execute(const std::string& query,
        CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
        bool assert_ok = true) {
        shared::StatementPtr statement = new Statement(cass_statement_new(query.c_str(), 0));
        EXPECT_EQ(CASS_OK, cass_statement_set_consistency(statement->statement_.get(),
          consistency));
        return execute(statement);
      }

      /**
       * Execute a batch statement asynchronously
       *
       * @param batch Batch statement to execute
       * @return Future object
       */
      shared::FuturePtr execute_async(shared::BatchPtr batch) {
        return new Future(cass_session_execute_batch(session_.get(),
          batch->batch_.get()));
      }

      /**
       * Execute a statement asynchronously
       *
       * @param statement Query or bound statement to execute
       * @return Future object
       */
      shared::FuturePtr execute_async(shared::StatementPtr statement) {
        return new Future(cass_session_execute(session_.get(),
          statement->statement_.get()));
      }

      /**
       * Execute a query asynchronously
       *
       * @param query Query to execute as a simple statement
       * @param consistency Consistency level to execute the query at
       *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
       * @return Future object
       */
      shared::FuturePtr execute_async(const std::string& query,
        CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE) {
        shared::StatementPtr statement = new Statement(cass_statement_new(query.c_str(), 0));
        EXPECT_EQ(CASS_OK, cass_statement_set_consistency(statement->statement_.get(),
          consistency));
        return execute_async(statement);
      }

      /**
       * Create a prepared statement
       *
       * @param query The query to prepare
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       * @return Prepared statement generated by the server for the query
       */
      shared::PreparedPtr prepare(const std::string& query, bool assert_ok = true) {
        Future prepare_future = cass_session_prepare(session_.get(), query.c_str());
        prepare_future.wait(assert_ok);
        return new Prepared(cass_future_get_prepared(prepare_future.future_.get()));
      }

    private:
      /**
       * Session driver reference object
       */
      shared::native::SessionPtr session_;

      /**
       * Create a new session and establish a connection to the server;
       * synchronously
       *
       * @param cluster Cluster configuration to use when establishing
       *                connection
       * @param keyspace Keyspace to use (default: None)
       * @return Session object
       * @throws Session::Exception If session could not be established
       */
      static SmartPtr<Session> connect(CassCluster* cluster, const std::string& keyspace) {
        SmartPtr<Session> session = new Session(cass_session_new());
        shared::FuturePtr connect_future;
        if (keyspace.empty()) {
          connect_future = new Future(cass_session_connect(session->session_.get(),
            cluster));
        } else {
          connect_future = new Future(cass_session_connect_keyspace(session->session_.get(),
            cluster, keyspace.c_str()));
        }
        connect_future->wait(false);
        if (connect_future->error_code() != CASS_OK) {
          throw Exception("Unable to Establish Session Connection: "
              + connect_future->error_description(), connect_future->error_code());
        }
        return session;
      }
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Session> SessionPtr;
    }
    namespace shared {
      typedef SmartPtr<Session> SessionPtr;
    }
  }
}

#endif // __DRIVER_OBJECT_SESSION_HPP__
