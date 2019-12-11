/*
  Copyright (c) DataStax, Inc.
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

#include "loop_test.hpp"

#include "execute_request.hpp"
#include "md5.hpp"
#include "prepared.hpp"
#include "session.hpp"
#include "set.hpp"
#include "uuids.hpp"

using namespace mockssandra;
using datastax::internal::ScopedMutex;
using datastax::internal::Set;
using datastax::internal::core::Config;
using datastax::internal::core::ExecuteRequest;
using datastax::internal::core::Future;
using datastax::internal::core::Prepared;
using datastax::internal::core::ResponseFuture;
using datastax::internal::core::ResultResponse;
using datastax::internal::core::Session;

#define PREPARED_QUERY "SELECT * FROM test"

class PreparedUnitTest : public LoopTest {
public:
  /**
   * A thread-safe class for tracking prepared statements. IDs are the MD5 of the query
   * (non-normalized, so spacing matters).
   *
   * Key: "127.0.0.1_<id>"
   */
  class PrepareStatements {
  public:
    PrepareStatements() { uv_mutex_init(&mutex_); }
    ~PrepareStatements() { uv_mutex_destroy(&mutex_); }

    String put_query(const Address& address, const String& query) {
      ScopedMutex l(&mutex_);
      String id = generate_id(query);
      statements_.insert(to_key(address, id));
      return id;
    }

    bool contains_id(const Address& address, const String& id) const {
      ScopedMutex l(&mutex_);
      return statements_.count(to_key(address, id)) > 0;
    }

    bool contains_query(const Address& address, const String& query) const {
      return contains_id(address, generate_id(query));
    }

  private:
    String to_key(const Address& address, const String& id) const {
      return address.to_string() + "_" + id;
    }

    String generate_id(const String& query) const {
      datastax::internal::Md5 m;
      m.update(reinterpret_cast<const uint8_t*>(query.data()), query.length());
      uint8_t hash[16];
      m.final(hash);
      return String(reinterpret_cast<const char*>(hash), 16);
    }

  private:
    mutable uv_mutex_t mutex_;
    Set<String> statements_;
  };

  /**
   * Action that handles PREPARE requests. It records prepared statements in an instance of
   * `PrepareStatements`.
   */
  class PrepareQuery : public Action {
  public:
    PrepareQuery(PrepareStatements* statements)
        : statements_(statements) {}

    void on_run(Request* request) const {
      String query;
      PrepareParameters params;
      if (!request->decode_prepare(&query, &params)) {
        request->error(ERROR_PROTOCOL_ERROR, "Invalid prepare message");
      } else {
        String id = statements_->put_query(request->address(), query);
        String body;
        encode_int32(RESULT_PREPARED, &body);
        encode_string(id, &body); // Prepared ID
        // Metadata
        encode_int32(0, &body); // Flags
        encode_int32(0, &body); // Column count
        encode_int32(0, &body); // Primary key count
        // Result metadata
        encode_int32(0, &body); // Flags
        encode_int32(0, &body); // Column count
        encode_int32(0, &body); // Primary key count
        request->write(OPCODE_RESULT, body);
      }
    }

  private:
    PrepareStatements* statements_;
  };

  /**
   * Action that handles EXECUTE requests. It checks a `PrepareStatements` instance and returns an
   * UNPREPARED error response if not prepared on the current node.
   */
  class ExecuteQuery : public Action {
  public:
    ExecuteQuery(PrepareStatements* statements)
        : statements_(statements) {}

    void on_run(Request* request) const {
      String id;
      String query;
      QueryParameters params;
      if (!request->decode_execute(&id, &params)) {
        request->error(ERROR_PROTOCOL_ERROR, "Invalid execute message");
      } else if (!statements_->contains_id(request->address(), id)) {
        String body;
        encode_int32(ERROR_UNPREPARED, &body);         // Error code
        encode_string("Prepared ID not found", &body); // Error message
        encode_string(id, &body);                      // Prepared ID
        request->write(OPCODE_ERROR, body);
      } else {
        String body;
        encode_int32(RESULT_ROWS, &body); // Result kind
        encode_int32(0, &body);           // Flags
        encode_int32(0, &body);           // Column count
        encode_int32(0, &body);           // Row count
        request->write(OPCODE_RESULT, body);
      }
    }

  private:
    PrepareStatements* statements_;
  };

  static void connect(const Config& config, Session* session,
                      uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    Future::Ptr connect_future(session->connect(config));
    ASSERT_TRUE(connect_future->wait_for(wait_for_time_us))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;
  }

  static void close(Session* session, uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    Future::Ptr close_future(session->close());
    ASSERT_TRUE(close_future->wait_for(wait_for_time_us))
        << "Timed out waiting for session to close";
    ASSERT_FALSE(close_future->error())
        << cass_error_desc(close_future->error()->code) << ": " << close_future->error()->message;
  }

  static Prepared::ConstPtr prepare(Session* session, const String& query) {
    ResponseFuture::Ptr future = session->prepare(query.c_str(), query.length());

    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME)) << "Timed out waiting to prepare query";
    EXPECT_FALSE(future->error()) << cass_error_desc(future->error()->code) << ": "
                                  << future->error()->message;

    ResultResponse::Ptr result(future->response());

    if (!result || result->kind() != CASS_RESULT_KIND_PREPARED) {
      return Prepared::ConstPtr();
    }

    return Prepared::ConstPtr(
        new Prepared(result, future->prepare_request, *future->schema_metadata));
  }
};

/**
 * Verify that statement is re-prepared on a node that doesn't have the prepared statement.
 */
TEST_F(PreparedUnitTest, ReprepareOnUnpreparedNode) {
  PrepareStatements statements;

  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(OPCODE_PREPARE).execute(new PrepareQuery(&statements));
  builder.on(OPCODE_EXECUTE).execute(new ExecuteQuery(&statements));

  mockssandra::SimpleCluster cluster(builder.build(), 2); // Requires at least 2 nodes
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_prepare_on_all_hosts(false); // Force re-prepare when executing on a new node
  config.contact_points().push_back(Address("127.0.0.1", 9042));

  Session session;
  connect(config, &session);

  Prepared::ConstPtr prepared = prepare(&session, PREPARED_QUERY);
  ASSERT_TRUE(prepared);

  {
    Future::Ptr future =
        session.execute(ExecuteRequest::ConstPtr(new ExecuteRequest(prepared.get())));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME)) << "Timed out waiting to execute prepared query ";
    EXPECT_FALSE(future->error()) << cass_error_desc(future->error()->code) << ": "
                                  << future->error()->message;
  }

  EXPECT_TRUE(statements.contains_query(Address("127.0.0.1", 9042), PREPARED_QUERY));
  EXPECT_TRUE(statements.contains_query(Address("127.0.0.2", 9042), PREPARED_QUERY));

  close(&session);
}
