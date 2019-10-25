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

#include "cluster.hpp"
#include "query_request.hpp"
#include "session_base.hpp"

#define KEYSPACE "datastax"

using namespace datastax;
using namespace datastax::internal::core;

class TestSessionBase : public SessionBase {
public:
  TestSessionBase()
      : connected_(0)
      , failed_(0)
      , closed_(0) {}

  virtual void on_host_up(const Host::Ptr& host) {}
  virtual void on_host_down(const Host::Ptr& host) {}
  virtual void on_host_added(const Host::Ptr& host) {}
  virtual void on_host_removed(const Host::Ptr& host) {}
  virtual void on_token_map_updated(const TokenMap::Ptr& token_map) {}

  int connected() { return connected_; }
  int failed() { return failed_; }
  int closed() { return closed_; }

protected:
  virtual void on_connect(const Host::Ptr& connected_host, ProtocolVersion protocol_version,
                          const HostMap& hosts, const TokenMap::Ptr& token_map,
                          const String& local_dc) {
    ++connected_;

    EXPECT_STREQ("127.0.0.1", connected_host->address_string().c_str());
    EXPECT_EQ(ProtocolVersion(PROTOCOL_VERSION), protocol_version);
    EXPECT_EQ(1u, hosts.size());
    EXPECT_EQ(state(), SESSION_STATE_CONNECTING);
    notify_connected();
  }

  virtual void on_connect_failed(CassError code, const String& message) {
    ++failed_;
    EXPECT_EQ(state(), SESSION_STATE_CONNECTING);
    notify_connect_failed(code, message);
    EXPECT_EQ(state(), SESSION_STATE_CLOSED);
  }

  virtual void on_close() {
    ++closed_;
    EXPECT_EQ(state(), SESSION_STATE_CLOSING);
    notify_closed();
  }

private:
  int connected_;
  int failed_;
  int closed_;
};

class SessionBaseUnitTest : public Unit {};

TEST_F(SessionBaseUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(session_base.state(), SessionBase::SESSION_STATE_CONNECTED);
  EXPECT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  EXPECT_NE(&session_base.config(), &config);
  EXPECT_TRUE(session_base.random() != NULL);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleEmptyKeyspaceWithoutRandom) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_use_randomized_contact_points(false);
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(session_base.state(), SessionBase::SESSION_STATE_CONNECTED);
  EXPECT_TRUE(session_base.connect_keyspace().empty());
  EXPECT_NE(&session_base.config(), &config);
  EXPECT_TRUE(session_base.random() == NULL);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple());
  ConnectionSettings settings(use_ssl(&cluster));
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_ssl_context(settings.socket_settings.ssl_context.get());
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(session_base.state(), SessionBase::SESSION_STATE_CONNECTED);
  EXPECT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  EXPECT_NE(&session_base.config(), &config);
  EXPECT_TRUE(session_base.random() != NULL);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleInvalidContactPointsIp) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_use_randomized_contact_points(false);
  config.contact_points().push_back(Address("123.456.789.012", 9042));
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  EXPECT_NE(&session_base.config(), &config);
  EXPECT_TRUE(session_base.random() == NULL);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleInvalidContactPointsHostname) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("doesnotexist.dne", 9042));
  config.contact_points().push_back(Address("localhost", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  EXPECT_NE(&session_base.config(), &config);
  EXPECT_TRUE(session_base.random() != NULL);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, InvalidProtocol) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.with_supported_protocol_versions(0, 0); // Don't support any valid protocol version
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, connect_future->error()->code);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(1, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, UnsupportedProtocol) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_protocol_version(ProtocolVersion(2)); // Unsupported protocol version
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_TRUE(connect_future->error()->message.find(
                  "Operation unsupported by this protocol version") != String::npos);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(1, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SslError) {
  mockssandra::SimpleCluster cluster(simple());
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  SslContext::Ptr invalid_ssl_context(SslContextFactory::create());
  invalid_ssl_context->set_verify_flags(CASS_SSL_VERIFY_PEER_CERT);
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_ssl_context(invalid_ssl_context.get());
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_SSL_INVALID_PEER_CERT, connect_future->error()->code);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(1, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_credentials("cassandra", "cassandra");
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, BadCredentials) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, connect_future->error()->code);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(1, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, NoHostsAvailable) {
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(1, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, ConnectWhenAlreadyConnected) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  {
    Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
    EXPECT_EQ(1, session_base.connected());
    EXPECT_EQ(0, session_base.failed());
    EXPECT_EQ(0, session_base.closed());
  }

  { // Attempt second session connection
    Future::Ptr connect_future(session_base.connect(config, ""));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
    EXPECT_EQ(CASS_ERROR_LIB_UNABLE_TO_CONNECT, connect_future->error()->code);
    EXPECT_EQ(1, session_base.connected());
    EXPECT_EQ(0, session_base.failed());
    EXPECT_EQ(0, session_base.closed());

    ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
    EXPECT_EQ(1, session_base.connected());
    EXPECT_EQ(0, session_base.failed());
    EXPECT_EQ(1, session_base.closed());
  }
}

TEST_F(SessionBaseUnitTest, CloseWhenAlreadyClosed) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  TestSessionBase session_base;

  Future::Ptr connect_future(session_base.connect(config, KEYSPACE));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());

  ASSERT_TRUE(session_base.close()->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());

  // Attempt second session close
  Future::Ptr close_future(session_base.close());
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_LIB_UNABLE_TO_CLOSE, close_future->error()->code);
  EXPECT_EQ(1, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, CloseWhenNotConnected) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  TestSessionBase session_base;

  Future::Ptr close_future(session_base.close());
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(CASS_ERROR_LIB_UNABLE_TO_CLOSE, close_future->error()->code);
  EXPECT_EQ(0, session_base.connected());
  EXPECT_EQ(0, session_base.failed());
  EXPECT_EQ(0, session_base.closed());
}
