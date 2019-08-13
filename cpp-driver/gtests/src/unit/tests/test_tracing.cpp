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

#include "unit.hpp"

#include "query_request.hpp"
#include "session.hpp"
#include "tracing_data_handler.hpp"

using namespace datastax::internal::core;

class TracingUnitTest : public Unit {
public:
  void TearDown() {
    ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
    Unit::TearDown();
  }

  void connect(const Config& config = Config()) {
    Config temp(config);
    temp.contact_points().push_back(Address("127.0.0.1", 9042));
    Future::Ptr connect_future(session.connect(temp));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;
  }

  Session session;
};

TEST_F(TracingUnitTest, Simple) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .system_local()
      .system_peers()
      .system_traces()
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  Statement::Ptr request(new QueryRequest("blah", 0));
  request->set_tracing(true);

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->response());
  EXPECT_TRUE(future->response()->has_tracing_id());

  CassUuid tracing_id(future->response()->tracing_id());
  EXPECT_NE(tracing_id.time_and_version, 0u);
}

TEST_F(TracingUnitTest, DataNotAvailble) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .system_local()
      .system_peers()
      .is_query(SELECT_TRACES_SESSION)
      .then(mockssandra::Action::Builder().empty_rows_result(0)) // Send back an empty row result
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  Statement::Ptr request(new QueryRequest("blah", 0));
  request->set_tracing(true);

  add_logging_critera("Tracing data not available after 15 ms");

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->response());
  EXPECT_TRUE(future->response()->has_tracing_id());

  CassUuid tracing_id(future->response()->tracing_id());
  EXPECT_NE(tracing_id.time_and_version, 0u);

  EXPECT_GT(logging_criteria_count(), 0);
}

TEST_F(TracingUnitTest, RequestTimeout) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .system_local()
      .system_peers()
      .is_query(SELECT_TRACES_SESSION)
      .then(mockssandra::Action::Builder().no_result()) // Don't send back a response
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_max_tracing_wait_time_ms(500);
  connect(config);

  Statement::Ptr request(new QueryRequest("blah", 0));
  request->set_request_timeout_ms(100);
  request->set_tracing(true);

  add_logging_critera("A query timeout occurred waiting for tracing data to become available");

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->response());
  EXPECT_TRUE(future->response()->has_tracing_id());

  CassUuid tracing_id(future->response()->tracing_id());
  EXPECT_NE(tracing_id.time_and_version, 0u);

  EXPECT_GT(logging_criteria_count(), 0);
}

TEST_F(TracingUnitTest, QueryError) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .system_local()
      .system_peers()
      .is_query(SELECT_TRACES_SESSION)
      .then(mockssandra::Action::Builder().error(mockssandra::ERROR_INVALID_QUERY, "Invalid query"))
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  Statement::Ptr request(new QueryRequest("blah", 0));
  request->set_tracing(true);

  add_logging_critera("Chained error response 'Invalid query' (0x02002200) for query "
                      "\"SELECT session_id FROM system_traces.sessions WHERE session_id = ?\"");

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->response());
  EXPECT_TRUE(future->response()->has_tracing_id());

  CassUuid tracing_id(future->response()->tracing_id());
  EXPECT_NE(tracing_id.time_and_version, 0u);

  EXPECT_GT(logging_criteria_count(), 0);
}
