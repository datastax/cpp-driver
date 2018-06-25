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

#include <gtest/gtest.h>

#include "cluster.hpp"
#include "query_request.hpp"
#include "mockssandra_test.hpp"
#include "session_base.hpp"

#define WAIT_FOR_TIME 5 * 1000 * 1000 // 5 seconds
#define KEYSPACE "datastax"

class TestSessionBase : public cass::SessionBase {
public:
  TestSessionBase()
    : connected_(0)
    , failed_(0)
    , closed_(0) { }

  virtual void on_up(const cass::Host::Ptr& host) { }
  virtual void on_down(const cass::Host::Ptr& host) { }
  virtual void on_add(const cass::Host::Ptr& host) { }
  virtual void on_remove(const cass::Host::Ptr& host) { }
  virtual void on_update_token_map(const cass::TokenMap::Ptr& token_map) { }

  int connected() { return connected_; }
  int failed() { return failed_; }
  int closed() { return closed_; }

protected:
  virtual void on_connect(const cass::Host::Ptr& connected_host,
                          int protocol_version,
                          const cass::HostMap& hosts,
                          const cass::TokenMap::Ptr& token_map) {
    ++connected_;
    ASSERT_STREQ("127.0.0.1", connected_host->address_string().c_str());
    ASSERT_EQ(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION, protocol_version);
    ASSERT_EQ(1, hosts.size());
    ASSERT_EQ(state(), SESSION_STATE_CONNECTING);
    notify_connected();
    ASSERT_EQ(state(), SESSION_STATE_CONNECTED);
  }


  virtual void on_connect_failed(CassError code, const cass::String& message) {
    ++failed_;
    ASSERT_EQ(state(), SESSION_STATE_CONNECTING);
    notify_connect_failed(code, message);
    ASSERT_EQ(state(), SESSION_STATE_CLOSED);
  }


  virtual void on_close(cass::Cluster* cluster) {
    ++closed_;
    ASSERT_EQ(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION, cluster->protocol_version());
    if (connected_ > 0) {
      ASSERT_STREQ("127.0.0.1", cluster->connected_host()->address_string().c_str());
      ASSERT_EQ(1, cluster->hosts().size());
    } else {
      ASSERT_STREQ("", cluster->connected_host()->address_string().c_str());
      ASSERT_EQ(0, cluster->hosts().size());
    }
    ASSERT_EQ(state(), SESSION_STATE_CLOSING);
    notify_closed();
    ASSERT_EQ(state(), SESSION_STATE_CLOSED);
  }

private:
  int connected_;
  int failed_;
  int closed_;
};

class SessionBaseUnitTest : public mockssandra::SimpleClusterTest { };

TEST_F(SessionBaseUnitTest, Simple) {
  start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  ASSERT_NE(&session_base.config(), &config);
  ASSERT_TRUE(session_base.random() != NULL);
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(0, session_base.closed());

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session_base.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleEmptyKeyspaceWithoutRandom) {
  start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  config.set_use_randomized_contact_points(false);
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, "", connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(session_base.connect_keyspace().empty());
  ASSERT_NE(&session_base.config(), &config);
  ASSERT_TRUE(session_base.random() == NULL);
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(0, session_base.closed());

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session_base.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, Ssl) {
  cass::ClusterSettings settings;
  settings.control_connection_settings.connection_settings = use_ssl();
  start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  config.set_ssl_context(settings.control_connection_settings.connection_settings.socket_settings.ssl_context.get());
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  ASSERT_NE(&session_base.config(), &config);
  ASSERT_TRUE(session_base.random() != NULL);
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(0, session_base.closed());

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session_base.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleInvalidContactPointsIp) {
  start_all();

  cass::Config config;
  config.set_use_randomized_contact_points(false);
  config.contact_points().push_back("123.456.789.012");
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  ASSERT_NE(&session_base.config(), &config);
  ASSERT_TRUE(session_base.random() != NULL);
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(0, session_base.closed());

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session_base.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SimpleInvalidContactPointsHostname) {
  start_all();

  cass::Config config;
  config.contact_points().push_back("doesnotexist.dne");
  config.contact_points().push_back("localhost");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_STREQ(KEYSPACE, session_base.connect_keyspace().c_str());
  ASSERT_NE(&session_base.config(), &config);
  ASSERT_TRUE(session_base.random() != NULL);
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(0, session_base.closed());

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session_base.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(1, session_base.connected());
  ASSERT_EQ(0, session_base.failed());
  ASSERT_EQ(1, session_base.closed());
}

TEST_F(SessionBaseUnitTest, InvalidProtocol) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.with_supported_protocol_versions(0, 0); // Don't support any valid protocol version
  mockssandra::SimpleCluster cluster(builder.build(), 1);
  cluster.start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, connect_future->error()->code);
  ASSERT_EQ(0, session_base.connected());
  ASSERT_EQ(1, session_base.failed());
  ASSERT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, SslError) {
  cass::ClusterSettings settings;
  settings.control_connection_settings.connection_settings = use_ssl();
  start_all();

  cass::SslContext::Ptr invalid_ssl_context(cass::SslContextFactory::create());
  invalid_ssl_context->set_verify_flags(CASS_SSL_VERIFY_PEER_CERT);
  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  config.set_ssl_context(invalid_ssl_context.get());
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_SSL_INVALID_PEER_CERT, connect_future->error()->code);
  ASSERT_EQ(0, session_base.connected());
  ASSERT_EQ(1, session_base.failed());
  ASSERT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, BadCredentials) {
  mockssandra::SimpleCluster cluster(mockssandra::AuthRequestHandlerBuilder().build());
  cluster.start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, connect_future->error()->code);
  ASSERT_EQ(0, session_base.connected());
  ASSERT_EQ(1, session_base.failed());
  ASSERT_EQ(0, session_base.closed());
}

TEST_F(SessionBaseUnitTest, NoHostsAvailable) {
  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  TestSessionBase session_base;

  session_base.connect(config, KEYSPACE, connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  ASSERT_EQ(0, session_base.connected());
  ASSERT_EQ(1, session_base.failed());
  ASSERT_EQ(0, session_base.closed());
}
