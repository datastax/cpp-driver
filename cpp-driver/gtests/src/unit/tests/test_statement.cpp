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
#include "control_connection.hpp"
#include "session.hpp"
#include "constants.hpp"

class StatementUnitTest : public Unit {
public:
  void TearDown() {
    ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
    Unit::TearDown();
  }

  void connect(const cass::Config& config = cass::Config()) {
    cass::Config temp(config);
    temp.contact_points().push_back("127.0.0.1");
    temp.contact_points().push_back("127.0.0.2"); // At least one more host (in case node 1 is down)
    cass::Future::Ptr connect_future(session.connect(temp));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error())
        << cass_error_desc(connect_future->error()->code) << ": "
        << connect_future->error()->message;
  }

  void get_rpc_address(const cass::Response::Ptr& response, cass::Address* output) {
    ASSERT_TRUE(response);
    ASSERT_EQ(response->opcode(), CQL_OPCODE_RESULT);

    cass::ResultResponse::Ptr result(response);

    const cass::Value* value = result->first_row().get_by_name("rpc_address");
    ASSERT_TRUE(value);
    ASSERT_EQ(value->value_type(), CASS_VALUE_TYPE_INET);

    CassInet inet;
    ASSERT_TRUE(value->decoder().as_inet(value->size(), &inet));

    ASSERT_TRUE(cass::Address::from_inet(inet.address, inet.address_length, 9042, output));
  }

  cass::Session session;
};

TEST_F(StatementUnitTest, SetHost) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  mockssandra::Ipv4AddressGenerator gen;

  for (int i = 0; i < 2; ++i) {
    cass::Address expected_host(gen.next());

    cass::Statement::Ptr request(new cass::QueryRequest(SELECT_LOCAL, 0));
    request->set_host(expected_host);

    cass::ResponseFuture::Ptr future(session.execute(cass::Request::ConstPtr(request)));
    future->wait();

    cass::Address actual_host;
    get_rpc_address(future->response(), &actual_host);

    EXPECT_EQ(expected_host, actual_host);
  }
}

TEST_F(StatementUnitTest, SetHostWithInvalidPort) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  connect();

  cass::Address expected_host(cass::Address("127.0.0.1", 8888)); // Invalid port

  cass::Statement::Ptr request(new cass::QueryRequest(SELECT_LOCAL, 0));
  request->set_host(expected_host);

  cass::ResponseFuture::Ptr future(session.execute(cass::Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

TEST_F(StatementUnitTest, SetHostWhereHostIsDown) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  cluster.stop(1);

  connect();

  cass::Address expected_host(cass::Address("127.0.0.1", 9042));

  cass::Statement::Ptr request(new cass::QueryRequest(SELECT_LOCAL, 0));
  request->set_host(expected_host);

  cass::ResponseFuture::Ptr future(session.execute(cass::Request::ConstPtr(request)));
  future->wait();

  ASSERT_TRUE(future->error());
  EXPECT_EQ(future->error()->code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}
