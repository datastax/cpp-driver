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

#include "query_request.hpp"
#include "session.hpp"
#include "uuids.hpp"

using namespace mockssandra;
using datastax::internal::core::Config;
using datastax::internal::core::Future;
using datastax::internal::core::QueryRequest;
using datastax::internal::core::Session;
using datastax::internal::core::UuidGen;

#define SELECT_LOCAL_SCHEMA_CHANGE "SELECT schema_version FROM system.local WHERE key='local'"
#define SELECT_PEERS_SCHEMA_CHANGE \
  "SELECT peer, rpc_address, host_id, schema_version FROM system.peers"

class SchemaAgreementUnitTest : public LoopTest {
public:
  static void connect(Session* session, uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    Config config;
    config.set_max_schema_wait_time_ms(500);
    config.contact_points().push_back(Address("127.0.0.1", 9042));
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

  static void execute(Session* session, const String& query) {
    Future::Ptr request_future(session->execute(QueryRequest::ConstPtr(new QueryRequest(query))));
    EXPECT_TRUE(request_future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(request_future->error()) << cass_error_desc(request_future->error()->code) << ": "
                                          << request_future->error()->message;
  }

  struct SchemaVersionCheckCounts {
    SchemaVersionCheckCounts()
        : local_count(0)
        , peers_count(0) {}
    Atomic<int> local_count;
    Atomic<int> peers_count;
  };

  class SystemSchemaVersion : public Action {
  public:
    enum AgreementType { NEVER_REACH_AGREEMENT, IMMEDIATE_AGREEMENT };

    SystemSchemaVersion(AgreementType type, SchemaVersionCheckCounts* counts)
        : type_(type)
        , check_counts_(counts) {
      uuid_gen_.generate_random(&uuid_);
    }

    void on_run(Request* request) const {
      String query;
      QueryParameters params;
      if (!request->decode_query(&query, &params)) {
        request->error(ERROR_PROTOCOL_ERROR, "Invalid query message");
      } else if (query.find(SELECT_LOCAL_SCHEMA_CHANGE) != String::npos) {
        ResultSet local_rs = ResultSet::Builder("system", "local")
                                 .column("schema_version", Type::uuid())
                                 .row(Row::Builder().uuid(generate_version()).build())
                                 .build();
        request->write(OPCODE_RESULT, local_rs.encode(request->version()));
        check_counts_->local_count.fetch_add(1);
      } else if (query.find(SELECT_PEERS_SCHEMA_CHANGE) != String::npos) {
        ResultSet::Builder peers_builder = ResultSet::Builder("system", "peers")
                                               .column("peer", Type::inet())
                                               .column("rpc_address", Type::inet())
                                               .column("host_id", Type::uuid())
                                               .column("schema_version", Type::uuid());
        Hosts hosts(request->hosts());
        for (Hosts::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
          const Host& host(*it);
          if (host.address == request->address()) {
            continue;
          }
          peers_builder.row(Row::Builder()
                                .inet(host.address)
                                .inet(host.address)
                                .uuid(generate_version()) // Doesn't matter
                                .uuid(generate_version())
                                .build());
        }
        ResultSet peers_rs = peers_builder.build();
        request->write(OPCODE_RESULT, peers_rs.encode(request->version()));
        check_counts_->peers_count.fetch_add(1);
      } else {
        run_next(request);
      }
    }

  private:
    CassUuid generate_version() const {
      CassUuid version;
      if (type_ == IMMEDIATE_AGREEMENT) {
        version = uuid_;
      } else {
        uuid_gen_.generate_random(&version);
      }
      return version;
    }

  private:
    AgreementType type_;
    CassUuid uuid_;
    SchemaVersionCheckCounts* check_counts_;
    mutable UuidGen uuid_gen_;
  };

  class SchemaChange : public Action {
  public:
    void on_run(Request* request) const {
      String query;
      QueryParameters params;
      if (!request->decode_query(&query, &params)) {
        request->error(ERROR_PROTOCOL_ERROR, "Invalid query message");
      } else if (query.find("CREATE TABLE") != String::npos) {
        request->write(OPCODE_RESULT, encode_schema_change("CREATE", "TABLE"));
      } else if (query.find("DROP TABLE") != String::npos) {
        request->write(OPCODE_RESULT, encode_schema_change("DROP", "TABLE"));
      } else {
        run_next(request);
      }
    }

  private:
    String encode_schema_change(String change_type, String target) const {
      String body;
      encode_int32(RESULT_SCHEMA_CHANGE, &body); // Result type
      encode_string(change_type, &body);
      encode_string("keyspace", &body);
      if (target == "TABLE") {
        encode_string("table", &body);
      }
      return body;
    }
  };
};

/**
 * Verify that schema changes wait for schema agreement.
 */
TEST_F(SchemaAgreementUnitTest, Simple) {
  SchemaVersionCheckCounts check_counts;

  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(OPCODE_QUERY)
      .execute(new SystemSchemaVersion(SystemSchemaVersion::IMMEDIATE_AGREEMENT, &check_counts))
      .execute(new SchemaChange())
      .system_local()
      .system_peers()
      .empty_rows_result(1);

  mockssandra::SimpleCluster cluster(builder.build(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);

  add_logging_critera("Found schema agreement in");

  execute(&session, "CREATE TABLE tbl (key text PRIMARY KEY, value text)");
  EXPECT_EQ(check_counts.local_count.load(), 1);
  EXPECT_EQ(check_counts.peers_count.load(), 1);
  EXPECT_EQ(logging_criteria_count(), 1);

  cluster.stop(2);
  // Give time for the session to see and react to the socket close, otherwise the next
  // query can wind up getting a "Request timed out" error if the close happens mid-flight
  test::Utils::msleep(100);
  execute(&session, "DROP TABLE tbl");
  EXPECT_EQ(check_counts.local_count.load(), 2);
  EXPECT_EQ(check_counts.peers_count.load(), 2);
  EXPECT_EQ(logging_criteria_count(), 2);

  cluster.stop(3);
  test::Utils::msleep(100);
  execute(&session, "CREATE TABLE tbl (key text PRIMARY KEY, value text)");
  EXPECT_EQ(check_counts.local_count.load(), 3);
  EXPECT_EQ(check_counts.peers_count.load(), 3);
  EXPECT_EQ(logging_criteria_count(), 3);

  close(&session);
}

/**
 * Verify that schema changes will timeout properly while waiting for schema agreement.
 */
TEST_F(SchemaAgreementUnitTest, Timeout) {
  SchemaVersionCheckCounts check_counts;

  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(OPCODE_QUERY)
      .execute(new SystemSchemaVersion(SystemSchemaVersion::NEVER_REACH_AGREEMENT, &check_counts))
      .execute(new SchemaChange())
      .system_local()
      .system_peers()
      .empty_rows_result(1);

  mockssandra::SimpleCluster cluster(builder.build(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);

  add_logging_critera("No schema agreement on live nodes after ");

  execute(&session, "CREATE TABLE tbl (key text PRIMARY KEY, value text)");

  EXPECT_GT(check_counts.local_count.load(), 1);
  EXPECT_GT(check_counts.peers_count.load(), 1);

  EXPECT_EQ(logging_criteria_count(), 1);

  close(&session);
}
