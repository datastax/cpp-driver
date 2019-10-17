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

#include "batch_request.hpp"
#include "constants.hpp"
#include "control_connection.hpp"
#include "query_request.hpp"
#include "session.hpp"

using namespace datastax::internal::core;

class StatementUnitTest : public Unit {
public:
  void TearDown() {
    ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
    Unit::TearDown();
  }

  void connect(const Config& config = Config()) {
    Config temp(config);
    temp.contact_points().push_back(Address("127.0.0.1", 9042));
    temp.contact_points().push_back(
        Address("127.0.0.2", 9042)); // At least one more host (in case node 1 is down)
    Future::Ptr connect_future(session.connect(temp));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;
  }

  void get_rpc_address(const Response::Ptr& response, Address* output) {
    ASSERT_TRUE(response);
    ASSERT_EQ(response->opcode(), CQL_OPCODE_RESULT);

    ResultResponse::Ptr result(response);

    const Value* value = result->first_row().get_by_name("rpc_address");
    ASSERT_TRUE(value);
    ASSERT_EQ(value->value_type(), CASS_VALUE_TYPE_INET);

    CassInet inet;
    ASSERT_TRUE(value->decoder().as_inet(value->size(), &inet));

    *output = Address(inet.address, inet.address_length, 9042);
    ASSERT_TRUE(output->is_valid_and_resolved());
  }

  Session session;
};

TEST_F(StatementUnitTest, SetHost) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  mockssandra::Ipv4AddressGenerator gen;

  for (int i = 0; i < 2; ++i) {
    Address expected_host(gen.next());

    Statement::Ptr request(new QueryRequest(SELECT_LOCAL, 0));
    request->set_host(expected_host);

    ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
    future->wait();

    Address actual_host;
    get_rpc_address(future->response(), &actual_host);

    EXPECT_EQ(expected_host, actual_host);
  }
}

TEST_F(StatementUnitTest, SetHostWithInvalidPort) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  Address expected_host(Address("127.0.0.1", 8888)); // Invalid port

  Statement::Ptr request(new QueryRequest(SELECT_LOCAL, 0));
  request->set_host(expected_host);

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

TEST_F(StatementUnitTest, SetHostWhereHostIsDown) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  cluster.stop(1);

  connect();

  Address expected_host(Address("127.0.0.1", 9042));

  Statement::Ptr request(new QueryRequest(SELECT_LOCAL, 0));
  request->set_host(expected_host);

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

TEST_F(StatementUnitTest, ErrorBatchWithNamedParameters) {
  mockssandra::SimpleCluster cluster(simple(), 1);
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  BatchRequest::Ptr batch(new BatchRequest(CASS_BATCH_TYPE_UNLOGGED));

  Statement::Ptr request(new QueryRequest("SELECT * FROM does_not_matter WHERE key = ?",
                                          1)); // Space for a named parameter

  request->set("key", 42); // Use named parameters

  batch->add_statement(request.get());

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(batch)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_BAD_PARAMS);
  EXPECT_EQ(future->error()->message, "Batches cannot contain queries with named values");
}

TEST_F(StatementUnitTest, ErrorParametersUnset) {
  mockssandra::SimpleCluster cluster(simple(), 1);
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_protocol_version(ProtocolVersion(3));

  connect(config);

  Statement::Ptr request(new QueryRequest("SELECT * FROM does_not_matter WHERE key = ?",
                                          1)); // Parameters start as unset

  ResponseFuture::Ptr future(session.execute(Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_PARAMETER_UNSET);
}
