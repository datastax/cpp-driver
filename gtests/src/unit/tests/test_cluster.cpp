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

#include "event_loop_test.hpp"
#include "test_utils.hpp"

#include "cluster_connector.hpp"
#include "ref_counted.hpp"

using namespace cass;

class ClusterUnitTest : public EventLoopTest {
public:
  ClusterUnitTest()
    : EventLoopTest("ClusterUnitTest") { }

  class Future : public cass::Future {
  public:
    typedef SharedRefPtr<Future> Ptr;

    Future()
      : cass::Future(FUTURE_TYPE_GENERIC) { }

    ~Future() {
      if (cluster_) {
        cluster_->close();
      }
    }

    const Cluster::Ptr& cluster() const { return cluster_; }

    void set_cluster(const Cluster::Ptr& cluster) {
      ScopedMutex lock(&mutex_);
      cluster_ = cluster;
      internal_set(lock);
    }

  private:
    Cluster::Ptr cluster_;
  };

  class Listener
      : public RefCounted<Listener>
      , public ClusterListener {
  public:
    typedef SharedRefPtr<Listener> Ptr;

    Listener(const Future::Ptr& close_future = Future::Ptr())
      : close_future_(close_future) {
      inc_ref(); // Might leak during an error, but won't allow a stale pointer
    }

    virtual ~Listener() { }

    virtual void on_up(const Host::Ptr& host) { }
    virtual void on_down(const Host::Ptr& host) { }

    virtual void on_add(const Host::Ptr& host) { }
    virtual void on_remove(const Host::Ptr& host) { }

    virtual void on_update_token_map(const TokenMap::Ptr& token_map) { }

    virtual void on_close(Cluster* cluster)  {
      if (close_future_) {
        close_future_->set();
      }
      dec_ref();
    }

  private:
    Future::Ptr close_future_;
  };

  class UpDownListener : public Listener {
  public:
    typedef SharedRefPtr<UpDownListener> Ptr;

    UpDownListener(const Future::Ptr& close_future,
                   const Future::Ptr& up_future,
                   const Future::Ptr& down_future)
      : Listener(close_future)
      , up_future_(up_future)
      , down_future_(down_future) {
      uv_mutex_init(&mutex_);
    }

    ~UpDownListener() {
      uv_mutex_destroy(&mutex_);
    }

    Address address() {
      ScopedMutex l(&mutex_);
      return address_;
    }

    virtual void on_up(const Host::Ptr& host) {
      if (up_future_) {
        ScopedMutex l(&mutex_);
        address_ = host->address();
        up_future_->set();
      }
    }

    virtual void on_down(const Host::Ptr& host) {
      if (down_future_) {
        ScopedMutex l(&mutex_);
        address_ = host->address();
        down_future_->set();
      }
    }

  private:
    uv_mutex_t mutex_;
    Future::Ptr up_future_;
    Future::Ptr down_future_;
    Address address_;
  };

  class ReconnectClusterListener : public Listener {
  public:
    typedef SharedRefPtr<ReconnectClusterListener> Ptr;

    struct Event {
      enum Type {
        NODE_ADD,
        NODE_REMOVE
      };
      Event(Type type, const Address& address)
        : type(type)
        , address(address) { }
      Type type;
      Address address;
    };
    typedef Vector<Event> Events;

    ReconnectClusterListener(const Future::Ptr& close_future, OutagePlan* outage_plan)
      : Listener(close_future)
      , outage_plan_(outage_plan) { }

    const HostVec& connected_hosts() const {
      return connected_hosts_;
    }

    const Events& events() const {
      return events_;
    }

    virtual void on_reconnect(Cluster* cluster) {
      connected_hosts_.push_back(cluster->connected_host());
      if (connected_hosts_.size() == 1) { // First host
        outage_plan_->run();
      } else {
        if (outage_plan_->is_done()) {
          outage_plan_->stop(); // Stop and clean up outage plan timer handle on the original event loop thread.
          cluster->close();
        }
      }
    }

    virtual void on_up(const Host::Ptr& host) { }
    virtual void on_down(const Host::Ptr& host) { }

    virtual void on_add(const Host::Ptr& host) {
      events_.push_back(Event(Event::NODE_ADD, host->address()));
    }

    virtual void on_remove(const Host::Ptr& host) {
      events_.push_back(Event(Event::NODE_REMOVE, host->address()));
    }

    virtual void on_update_token_map(const TokenMap::Ptr& token_map) { }

  private:
    HostVec connected_hosts_;
    Events events_;
    OutagePlan* outage_plan_;
  };

  class RecoverClusterListener : public UpDownListener {
  public:
    typedef SharedRefPtr<RecoverClusterListener> Ptr;

    RecoverClusterListener(const Future::Ptr& close_future,
                           const Future::Ptr& up_future,
                           const Future::Ptr& recover_future)
      : UpDownListener(close_future, up_future, Future::Ptr())
      , recover_future_(recover_future) { }

    const HostVec& connected_hosts() const {
      return connected_hosts_;
    }

    virtual void on_reconnect(Cluster* cluster) {
      connected_hosts_.push_back(cluster->connected_host());
      if (connected_hosts_.size() > 1 && recover_future_) {
        recover_future_->set();
      }
    }

  private:
    HostVec connected_hosts_;
    Future::Ptr recover_future_;
  };

  static void on_connection_connected(ClusterConnector* connector, Future* future) {
    if (connector->is_ok()) {
      future->set();
    } else {
      switch (connector->error_code()) {
        case ClusterConnector::CLUSTER_ERROR_INVALID_PROTOCOL:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL,
                            connector->error_message());
          break;
        case ClusterConnector::CLUSTER_ERROR_SSL_ERROR:
          future->set_error(connector->ssl_error_code(),
                            connector->error_message());
          break;
        case ClusterConnector::CLUSTER_ERROR_AUTH_ERROR:
          future->set_error(CASS_ERROR_SERVER_BAD_CREDENTIALS,
                            connector->error_message());
          break;
        case ClusterConnector::CLUSTER_ERROR_NO_HOSTS_AVAILABLE:
          future->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                            connector->error_message());
          break;
        case ClusterConnector::CLUSTER_CANCELED:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                            "Canceled");
          break;
        default:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                            connector->error_message());
          break;
      }
    }
  }

  static void on_connection_reconnect(ClusterConnector* connector, Future* future) {
    if (connector->is_ok()) {
      future->set_cluster(connector->release_cluster()); // Keep the cluster alive
    } else {
      future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                        connector->error_message());
    }
  }
};

TEST_F(ClusterUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  connector->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
}

TEST_F(ClusterUnitTest, Resolve) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("localhost");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));
  connector->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
}

TEST_F(ClusterUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  ClusterSettings settings;
  settings.control_connection_settings.connection_settings.auth_provider.reset(
        Memory::allocate<PlainTextAuthProvider>("cassandra", "cassandra"));

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
}

TEST_F(ClusterUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ClusterSettings settings;
  settings.control_connection_settings.connection_settings = use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
}

TEST_F(ClusterUnitTest, Cancel) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<Future::Ptr> connect_futures;
  Vector<ClusterConnector::Ptr> connectors;

  ContactPointList contact_points;
  contact_points.push_back("localhost");
  contact_points.push_back("google.com");
  contact_points.push_back("doesnotexist.dne");

  for (size_t i = 0; i < 10; ++i) {
    Future::Ptr connect_future(Memory::allocate<Future>());
    ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                       PROTOCOL_VERSION,
                                                                       bind_callback(on_connection_connected, connect_future.get())));
    connector->connect(event_loop());
    connectors.push_back(connector);
    connect_futures.push_back(connect_future);
  }

  for (Vector<ClusterConnector::Ptr>::iterator it = connectors.begin(),
       end = connectors.end(); it != end; ++it) {
    (*it)->cancel();
  }

  bool is_canceled = false;
  for (Vector<Future::Ptr>::const_iterator it = connect_futures.begin(),
       end = connect_futures.end(); it != end; ++it) {
    ASSERT_TRUE((*it)->wait_for(WAIT_FOR_TIME));
    if ((*it)->error() &&
        (*it)->error()->code == CASS_ERROR_LIB_UNABLE_TO_CONNECT &&
        (*it)->error()->message == "Canceled") {
      is_canceled = true;
    }
  }

  EXPECT_TRUE(is_canceled);
}

TEST_F(ClusterUnitTest, ReconnectToDiscoveredHosts) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(event_loop()->loop(), &cluster);

  // Full rolling restart
  outage_plan.stop_node(1);
  outage_plan.stop_node(2);
  outage_plan.start_node(1);
  outage_plan.stop_node(3);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_reconnect, connect_future.get())));
  ReconnectClusterListener::Ptr listener(
        Memory::allocate<ReconnectClusterListener>(close_future, &outage_plan));

  ClusterSettings settings;
  settings.reconnect_timeout_ms = 1; // Reconnect immediately
  settings.control_connection_settings.connection_settings.connect_timeout_ms = 200; // Give enough time for the connection to complete

  connector
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));

  ASSERT_EQ(listener->connected_hosts().size(), 4u);
  EXPECT_EQ(listener->connected_hosts()[0]->address(), Address("127.0.0.1", PORT));
  EXPECT_EQ(listener->connected_hosts()[1]->address(), Address("127.0.0.2", PORT));
  EXPECT_EQ(listener->connected_hosts()[2]->address(), Address("127.0.0.3", PORT));
  EXPECT_EQ(listener->connected_hosts()[3]->address(), Address("127.0.0.1", PORT));
}

TEST_F(ClusterUnitTest, ReconnectUpdateHosts) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(event_loop()->loop(), &cluster);

  // Add/Remove entries from the "system" tables
  outage_plan.remove_node(2);
  outage_plan.stop_node(1);
  outage_plan.add_node(2);
  outage_plan.start_node(1);
  outage_plan.stop_node(3);
  outage_plan.stop_node(1);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_reconnect, connect_future.get())));
  ReconnectClusterListener::Ptr listener(
        Memory::allocate<ReconnectClusterListener>(close_future, &outage_plan));

  ClusterSettings settings;
  settings.reconnect_timeout_ms = 1; // Reconnect immediately
  settings.control_connection_settings.connection_settings.connect_timeout_ms = 200; // Give enough time for the connection to complete

  connector
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));

  ASSERT_EQ(listener->connected_hosts().size(), 4u);
  EXPECT_EQ(listener->connected_hosts()[0]->address(), Address("127.0.0.1", PORT));
  EXPECT_EQ(listener->connected_hosts()[1]->address(), Address("127.0.0.3", PORT));
  EXPECT_EQ(listener->connected_hosts()[2]->address(), Address("127.0.0.1", PORT));
  EXPECT_EQ(listener->connected_hosts()[3]->address(), Address("127.0.0.2", PORT));

  // Events are triggered by the reconnect
  ASSERT_EQ(listener->events().size(), 2u);
  EXPECT_EQ(listener->events()[0].type, ReconnectClusterListener::Event::NODE_REMOVE);
  EXPECT_EQ(listener->events()[0].address, Address("127.0.0.2", PORT));
  EXPECT_EQ(listener->events()[1].type, ReconnectClusterListener::Event::NODE_ADD);
  EXPECT_EQ(listener->events()[1].address, Address("127.0.0.2", PORT));
}

TEST_F(ClusterUnitTest, CloseDuringReconnect) {
  mockssandra::SimpleCluster mock_cluster(simple());
  mock_cluster.start_all();

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_reconnect, connect_future.get())));

  Listener::Ptr listener(Memory::allocate<Listener>(close_future));

  ClusterSettings settings;
  settings.reconnect_timeout_ms = 100000; // Make sure we're reconnecting when we close.

  connector
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  Cluster::Ptr cluster(connect_future->cluster());

  mock_cluster.stop(1);
  test::Utils::msleep(200); // Give time for the reconnect to start
  cluster->close();

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(ClusterUnitTest, NotifyDownUp) {
  mockssandra::SimpleCluster mock_cluster(simple(), 3);
  mock_cluster.start_all();

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  Future::Ptr up_future(Memory::allocate<Future>());
  Future::Ptr down_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_reconnect, connect_future.get())));

  UpDownListener::Ptr listener(Memory::allocate<UpDownListener>(close_future, up_future, down_future));

  connector
      ->with_listener(listener.get())
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  Address address("127.0.0.1", PORT);

  Cluster::Ptr cluster(connect_future->cluster());

  // We need to mark the host as DOWN first otherwise an UP event won't be
  // triggered.
  cluster->notify_down(address);
  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(address, listener->address());

  cluster->notify_up(address);
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(address, listener->address());

  cluster->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(ClusterUnitTest, ProtocolNegotiation) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.with_supported_protocol_versions(1, PROTOCOL_VERSION - 1); // Support one less than our current version
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  connector->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  EXPECT_EQ(connector->protocol_version(), PROTOCOL_VERSION - 1);
}

TEST_F(ClusterUnitTest, NoSupportedProtocols) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.with_supported_protocol_versions(0, 0); // Don't support any valid protocol version
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  connector->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, connect_future->error()->code);
}

TEST_F(ClusterUnitTest, FindValidHost) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.99.99.1"); // Invalid
  contact_points.push_back("127.99.99.2"); // Invalid
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  ClusterSettings settings;
  settings.control_connection_settings.connection_settings.connect_timeout_ms = 100;

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
}

TEST_F(ClusterUnitTest, NoHostsAvailable) {
  // Don't start the cluster

  // Try multiple hosts
  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");
  contact_points.push_back("127.0.0.2");
  contact_points.push_back("127.0.0.3");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  connector->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
}

TEST_F(ClusterUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  ClusterSettings settings;
  settings.control_connection_settings.connection_settings.auth_provider.reset(
        Memory::allocate<PlainTextAuthProvider>("invalid", "invalid"));

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, connect_future->error()->code);
}

TEST_F(ClusterUnitTest, InvalidSsl) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ClusterSettings settings;
  settings.control_connection_settings.connection_settings.socket_settings.ssl_context = ssl_context;

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_SSL_INVALID_PEER_CERT, connect_future->error()->code);
}

TEST_F(ClusterUnitTest, DCAwareRecoverOnRemoteHost) {
  mockssandra::SimpleCluster cluster(simple(), 1, 1); // 2 DCs with a single node each
  ASSERT_EQ(cluster.start_all(), 0);

  Address local_address("127.0.0.1", 9042);
  Address remote_address("127.0.0.2", 9042);

  ContactPointList contact_points;
  contact_points.push_back(local_address.to_string());

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_reconnect, connect_future.get())));

  Future::Ptr up_future(Memory::allocate<Future>());
  Future::Ptr recover_future(Memory::allocate<Future>());
  RecoverClusterListener::Ptr listener(
        Memory::allocate<RecoverClusterListener>(close_future, up_future, recover_future));

  ClusterSettings settings;
  settings.load_balancing_policy.reset(Memory::allocate<DCAwarePolicy>("dc1", 1, false)); // Allow connection to a single remote host
  settings.load_balancing_policies.clear();
  settings.load_balancing_policies.push_back(settings.load_balancing_policy);
  settings.reconnect_timeout_ms = 1; // Reconnect immediately
  settings.control_connection_settings.connection_settings.connect_timeout_ms = 200; // Give enough time for the connection to complete

  connector
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  // Notify every host as down
  connect_future->cluster()->notify_down(local_address);
  connect_future->cluster()->notify_down(remote_address);

  // Notify the remote host as up
  connect_future->cluster()->notify_up(remote_address);

  // Verify that the remote host was marked as up
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(remote_address, listener->address());

  cluster.stop(1); // Stop local node to verify that remote host is tried for reconnection.
  ASSERT_TRUE(recover_future->wait_for(WAIT_FOR_TIME));

  connect_future->cluster()->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));

  ASSERT_EQ(listener->connected_hosts().size(), 2u);
  EXPECT_EQ(listener->connected_hosts()[0]->address(), Address("127.0.0.1", PORT));
  EXPECT_EQ(listener->connected_hosts()[1]->address(), Address("127.0.0.2", PORT)); // Connected to remote host.
}

TEST_F(ClusterUnitTest, InvalidDC) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  ContactPointList contact_points;
  contact_points.push_back("127.0.0.1");

  Future::Ptr connect_future(Memory::allocate<Future>());
  ClusterConnector::Ptr connector(Memory::allocate<ClusterConnector>(contact_points,
                                                                     PROTOCOL_VERSION,
                                                                     bind_callback(on_connection_connected, connect_future.get())));

  ClusterSettings settings;
  settings.load_balancing_policy.reset(Memory::allocate<DCAwarePolicy>("invalid_dc", 0, false)); // Invalid DC and not using remote hosts
  settings.load_balancing_policies.clear();
  settings.load_balancing_policies.push_back(settings.load_balancing_policy);

  connector
      ->with_settings(settings)
      ->connect(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_TRUE(connect_future->error()->message.find("Check to see if the configured local datacenter is valid") != String::npos);
}
