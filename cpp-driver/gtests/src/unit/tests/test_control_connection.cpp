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

#include "constants.hpp"
#include "control_connector.hpp"

#ifdef WIN32
#undef STATUS_TIMEOUT
#endif

using namespace datastax::internal;
using namespace datastax::internal::core;

using mockssandra::SchemaChangeEvent;
using mockssandra::StatusChangeEvent;
using mockssandra::TopologyChangeEvent;

struct RecordedEvent {
  enum Type {
    INVALID, // Used for the default constructor
    KEYSPACE_UPDATED,
    TABLE_UPDATED,
    VIEW_UPDATED,
    COLUMN_UPDATED,
    INDEX_UPDATED,
    USER_TYPE_UPDATED,
    FUNCTION_UPDATED,
    AGGREGATE_UPDATED,
    KEYSPACE_DROPPED,
    TABLE_DROPPED,
    VIEW_DROPPED,
    COLUMN_DROPPED,
    INDEX_DROPPED,
    USER_TYPE_DROPPED,
    FUNCTION_DROPPED,
    AGGREGATE_DROPPED,
    NODE_UP,
    NODE_DOWN,
    NODE_ADDED,
    NODE_REMOVED
  };

  RecordedEvent()
      : type(INVALID) {}
  RecordedEvent(Type type)
      : type(type) {}

  Type type;
  ResultResponse::Ptr result;
  String keyspace_name;
  String target_name;
  Host::Ptr host;
};

static RecordedEvent invalid_event__;

typedef Vector<RecordedEvent> RecordedEventVec;

class RecordingControlConnectionListener : public ControlConnectionListener {
public:
  const RecordedEventVec& events() const { return events_; }

  const RecordedEvent& find_event(RecordedEvent::Type type) {
    for (RecordedEventVec::const_iterator it = events().begin(), end = events().end(); it != end;
         ++it) {
      if (it->type == type) {
        return *it;
      }
    }
    return invalid_event__;
  }

protected:
  virtual void on_update_schema(SchemaType type, const ResultResponse::Ptr& result,
                                const String& keyspace_name, const String& target_name) {
    RecordedEvent event;
    switch (type) {
      case KEYSPACE:
        event.type = RecordedEvent::KEYSPACE_UPDATED;
        break;
      case TABLE:
        event.type = RecordedEvent::TABLE_UPDATED;
        break;
      case VIEW:
        event.type = RecordedEvent::VIEW_UPDATED;
        break;
      case COLUMN:
        event.type = RecordedEvent::COLUMN_UPDATED;
        break;
      case INDEX:
        event.type = RecordedEvent::INDEX_UPDATED;
        break;
      case USER_TYPE:
        event.type = RecordedEvent::USER_TYPE_UPDATED;
        break;
      case FUNCTION:
        event.type = RecordedEvent::FUNCTION_UPDATED;
        break;
      case AGGREGATE:
        event.type = RecordedEvent::AGGREGATE_UPDATED;
        break;
    }
    event.result = result;
    event.keyspace_name = keyspace_name;
    event.target_name = target_name;
    events_.push_back(event);
  }

  virtual void on_drop_schema(SchemaType type, const String& keyspace_name,
                              const String& target_name) {
    RecordedEvent event;
    switch (type) {
      case KEYSPACE:
        event.type = RecordedEvent::KEYSPACE_DROPPED;
        break;
      case TABLE:
        event.type = RecordedEvent::TABLE_DROPPED;
        break;
      case VIEW:
        event.type = RecordedEvent::VIEW_DROPPED;
        break;
      case COLUMN:
        event.type = RecordedEvent::COLUMN_DROPPED;
        break;
      case INDEX:
        event.type = RecordedEvent::INDEX_DROPPED;
        break;
      case USER_TYPE:
        event.type = RecordedEvent::USER_TYPE_DROPPED;
        break;
      case FUNCTION:
        event.type = RecordedEvent::FUNCTION_DROPPED;
        break;
      case AGGREGATE:
        event.type = RecordedEvent::AGGREGATE_DROPPED;
        break;
    }
    event.keyspace_name = keyspace_name;
    event.target_name = target_name;
    events_.push_back(event);
  }

  virtual void on_up(const Address& address) {
    RecordedEvent event(RecordedEvent::NODE_UP);
    event.host.reset(new Host(address));
    events_.push_back(event);
  }

  virtual void on_down(const Address& address) {
    RecordedEvent event(RecordedEvent::NODE_DOWN);
    event.host.reset(new Host(address));
    events_.push_back(event);
  }

  virtual void on_add(const Host::Ptr& host) {
    RecordedEvent event(RecordedEvent::NODE_ADDED);
    event.host = host;
    events_.push_back(event);
  }

  virtual void on_remove(const Address& address) {
    RecordedEvent event(RecordedEvent::NODE_REMOVED);
    event.host.reset(new Host(address));
    events_.push_back(event);
  }

  virtual void on_close(ControlConnection* connection) {}

private:
  RecordedEventVec events_;
};

class ControlConnectionUnitTest : public LoopTest {
public:
  static void on_connection_connected(ControlConnector* connector, bool* is_connected) {
    if (connector->is_ok()) {
      *is_connected = true;
    }
  }

  static void on_connection_close(ControlConnector* connector, bool* is_closed) {
    if (connector->error_code() == ControlConnector::CONTROL_CONNECTION_ERROR_CLOSE) {
      *is_closed = true;
    }
  }

  static void on_connection_error_code(ControlConnector* connector,
                                       ControlConnector::ControlConnectionError* error_code) {
    if (!connector->is_ok()) {
      *error_code = connector->error_code();
    }
  }

  struct EventListener : public RecordingControlConnectionListener {
  public:
    EventListener(mockssandra::SimpleCluster* cluster)
        : remaining_(0)
        , cluster_(cluster) {}

    void add_event(const mockssandra::Event::Ptr& event) { events_.push_back(event); }

    void trigger_events(const ControlConnection::Ptr& connection) {
      connection_ = connection;
      remaining_ = events_.size();
      for (Vector<mockssandra::Event::Ptr>::const_iterator it = events_.begin(),
                                                           end = events_.end();
           it != end; ++it) {
        cluster_->event(*it);
      }
    }

    virtual void on_update_schema(SchemaType type, const ResultResponse::Ptr& result,
                                  const String& keyspace_name, const String& target_name) {
      RecordingControlConnectionListener::on_update_schema(type, result, keyspace_name,
                                                           target_name);
      if (type == COLUMN || type == INDEX) return;
      if (--remaining_ <= 0) connection_->close();
    }

    virtual void on_drop_schema(SchemaType type, const String& keyspace_name,
                                const String& target_name = "") {
      RecordingControlConnectionListener::on_drop_schema(type, keyspace_name, target_name);
      if (--remaining_ <= 0) connection_->close();
    }

    virtual void on_up(const Address& address) {
      RecordingControlConnectionListener::on_up(address);
      if (--remaining_ <= 0) connection_->close();
    }

    virtual void on_down(const Address& address) {
      RecordingControlConnectionListener::on_down(address);
      if (--remaining_ <= 0) connection_->close();
    }

    virtual void on_add(const Host::Ptr& host) {
      RecordingControlConnectionListener::on_add(host);
      if (--remaining_ <= 0) connection_->close();
    }

    virtual void on_remove(const Address& address) {
      RecordingControlConnectionListener::on_remove(address);
      if (--remaining_ <= 0) connection_->close();
    }

  private:
    Vector<mockssandra::Event::Ptr> events_;
    int remaining_;
    mockssandra::SimpleCluster* cluster_;
    ControlConnection::Ptr connection_;
  };

  static void on_connection_event(ControlConnector* connector, EventListener* listener) {
    listener->trigger_events(connector->release_connection());
  }
};

TEST_F(ControlConnectionUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  bool is_connected = false;
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_connection_connected, &is_connected)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_connected);
}

TEST_F(ControlConnectionUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  bool is_connected = false;
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_connection_connected, &is_connected)));
  ControlConnectionSettings settings;
  settings.connection_settings.auth_provider.reset(
      new PlainTextAuthProvider("cassandra", "cassandra"));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_connected);
}

TEST_F(ControlConnectionUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple());
  ControlConnectionSettings settings;
  settings.connection_settings = use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  bool is_connected = false;
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_connection_connected, &is_connected)));
  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_connected);
}

TEST_F(ControlConnectionUnitTest, Close) {
  mockssandra::SimpleCluster cluster(simple());
  cluster.use_close_immediately();
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<ControlConnector::Ptr> connectors;

  bool is_closed(false);
  for (size_t i = 0; i < 10; ++i) {
    ControlConnector::Ptr connector(
        new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                             bind_callback(on_connection_close, &is_closed)));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(ControlConnectionUnitTest, Cancel) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<ControlConnector::Ptr> connectors;

  ControlConnector::ControlConnectionError error_code(ControlConnector::CONTROL_CONNECTION_OK);
  for (size_t i = 0; i < 10; ++i) {
    ControlConnector::Ptr connector(
        new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                             bind_callback(on_connection_error_code, &error_code)));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  Vector<ControlConnector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(ControlConnector::CONTROL_CONNECTION_CANCELED, error_code);
}

TEST_F(ControlConnectionUnitTest, StatusChangeEvents) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Address address("127.0.0.1", PORT);

  EventListener listener(&cluster);

  listener.add_event(StatusChangeEvent::up(address));
  listener.add_event(StatusChangeEvent::down(address));

  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(address)), PROTOCOL_VERSION,
                           bind_callback(on_connection_event, &listener)));
  connector->with_listener(&listener)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(2u, listener.events().size());

  const RecordedEvent& event1 = listener.find_event(RecordedEvent::NODE_UP);
  EXPECT_EQ(RecordedEvent::NODE_UP, event1.type);
  EXPECT_EQ(address, event1.host->address());

  const RecordedEvent& event2 = listener.find_event(RecordedEvent::NODE_DOWN);
  EXPECT_EQ(RecordedEvent::NODE_DOWN, event2.type);
  EXPECT_EQ(address, event2.host->address());
}

TEST_F(ControlConnectionUnitTest, TopologyChangeEvents) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  Address address1("127.0.0.1", PORT);
  Address address2("127.0.0.2", PORT);

  EventListener listener(&cluster);

  listener.add_event(TopologyChangeEvent::new_node(address2));
  listener.add_event(TopologyChangeEvent::removed_node(address2));

  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(address1)), PROTOCOL_VERSION,
                           bind_callback(on_connection_event, &listener)));
  connector->with_listener(&listener)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(2u, listener.events().size());

  const RecordedEvent& event1 = listener.find_event(RecordedEvent::NODE_ADDED);
  EXPECT_EQ(RecordedEvent::NODE_ADDED, event1.type);
  EXPECT_EQ(address2, event1.host->address());
  EXPECT_EQ("dc1", event1.host->dc());
  EXPECT_EQ("rack1", event1.host->rack());
  EXPECT_GT(event1.host->tokens().size(), 0u);

  const RecordedEvent& event2 = listener.find_event(RecordedEvent::NODE_REMOVED);
  EXPECT_EQ(RecordedEvent::NODE_REMOVED, event2.type);
  EXPECT_EQ(address2, event2.host->address());
}

TEST_F(ControlConnectionUnitTest, SchemaChangeEvents) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Address address("127.0.0.1", PORT);

  EventListener listener(&cluster);

  listener.add_event(SchemaChangeEvent::keyspace(SchemaChangeEvent::UPDATED, "keyspace1"));
  listener.add_event(SchemaChangeEvent::keyspace(SchemaChangeEvent::DROPPED, "keyspace1"));

  listener.add_event(SchemaChangeEvent::table(SchemaChangeEvent::UPDATED, "keyspace1", "table1"));
  listener.add_event(SchemaChangeEvent::table(SchemaChangeEvent::DROPPED, "keyspace1", "table1"));

  listener.add_event(
      SchemaChangeEvent::user_type(SchemaChangeEvent::UPDATED, "keyspace1", "type1"));
  listener.add_event(
      SchemaChangeEvent::user_type(SchemaChangeEvent::DROPPED, "keyspace1", "type1"));

  listener.add_event(SchemaChangeEvent::function(SchemaChangeEvent::UPDATED, "keyspace1",
                                                 "function1", Vector<String>(1, "int")));
  listener.add_event(SchemaChangeEvent::function(SchemaChangeEvent::DROPPED, "keyspace1",
                                                 "function1", Vector<String>(1, "int")));

  listener.add_event(SchemaChangeEvent::aggregate(SchemaChangeEvent::UPDATED, "keyspace1",
                                                  "aggregate1", Vector<String>(1, "varchar")));
  listener.add_event(SchemaChangeEvent::aggregate(SchemaChangeEvent::DROPPED, "keyspace1",
                                                  "aggregate1", Vector<String>(1, "varchar")));

  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(address)), PROTOCOL_VERSION,
                           bind_callback(on_connection_event, &listener)));
  connector->with_listener(&listener)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(12u, listener.events().size());

  const RecordedEvent& event1 = listener.find_event(RecordedEvent::KEYSPACE_UPDATED);
  EXPECT_EQ(RecordedEvent::KEYSPACE_UPDATED, event1.type);
  EXPECT_EQ("keyspace1", event1.keyspace_name);
  EXPECT_TRUE(event1.target_name.empty());
  EXPECT_TRUE(event1.result);

  const RecordedEvent& event2 = listener.find_event(RecordedEvent::KEYSPACE_DROPPED);
  EXPECT_EQ(RecordedEvent::KEYSPACE_DROPPED, event2.type);
  EXPECT_EQ("keyspace1", event2.keyspace_name);
  EXPECT_TRUE(event2.target_name.empty());

  const RecordedEvent& event3 = listener.find_event(RecordedEvent::TABLE_UPDATED);
  EXPECT_EQ(RecordedEvent::TABLE_UPDATED, event3.type);
  EXPECT_EQ("keyspace1", event3.keyspace_name);
  EXPECT_EQ("table1", event3.target_name);
  EXPECT_TRUE(event3.result);

  const RecordedEvent& event4 = listener.find_event(RecordedEvent::COLUMN_UPDATED);
  EXPECT_EQ(RecordedEvent::COLUMN_UPDATED, event4.type);
  EXPECT_EQ("keyspace1", event4.keyspace_name);
  EXPECT_EQ("table1", event4.target_name);
  EXPECT_TRUE(event4.result);

  const RecordedEvent& event5 = listener.find_event(RecordedEvent::INDEX_UPDATED);
  EXPECT_EQ(RecordedEvent::INDEX_UPDATED, event5.type);
  EXPECT_EQ("keyspace1", event5.keyspace_name);
  EXPECT_EQ("table1", event5.target_name);
  EXPECT_TRUE(event5.result);

  const RecordedEvent& event6 = listener.find_event(RecordedEvent::TABLE_DROPPED);
  EXPECT_EQ(RecordedEvent::TABLE_DROPPED, event6.type);
  EXPECT_EQ("keyspace1", event6.keyspace_name);
  EXPECT_EQ("table1", event6.target_name);

  const RecordedEvent& event7 = listener.find_event(RecordedEvent::USER_TYPE_UPDATED);
  EXPECT_EQ(RecordedEvent::USER_TYPE_UPDATED, event7.type);
  EXPECT_EQ("keyspace1", event7.keyspace_name);
  EXPECT_EQ("type1", event7.target_name);
  EXPECT_TRUE(event7.result);

  const RecordedEvent& event8 = listener.find_event(RecordedEvent::USER_TYPE_DROPPED);
  EXPECT_EQ(RecordedEvent::USER_TYPE_DROPPED, event8.type);
  EXPECT_EQ("keyspace1", event8.keyspace_name);
  EXPECT_EQ("type1", event8.target_name);

  const RecordedEvent& event9 = listener.find_event(RecordedEvent::FUNCTION_UPDATED);
  EXPECT_EQ(RecordedEvent::FUNCTION_UPDATED, event9.type);
  EXPECT_EQ("keyspace1", event9.keyspace_name);
  EXPECT_EQ("function1(int)", event9.target_name);
  EXPECT_TRUE(event9.result);

  const RecordedEvent& event10 = listener.find_event(RecordedEvent::FUNCTION_DROPPED);
  EXPECT_EQ(RecordedEvent::FUNCTION_DROPPED, event10.type);
  EXPECT_EQ("keyspace1", event10.keyspace_name);
  EXPECT_EQ("function1(int)", event10.target_name);

  const RecordedEvent& event11 = listener.find_event(RecordedEvent::AGGREGATE_UPDATED);
  EXPECT_EQ(RecordedEvent::AGGREGATE_UPDATED, event11.type);
  EXPECT_EQ("keyspace1", event11.keyspace_name);
  EXPECT_EQ("aggregate1(varchar)", event11.target_name);
  EXPECT_TRUE(event11.result);

  const RecordedEvent& event12 = listener.find_event(RecordedEvent::AGGREGATE_DROPPED);
  EXPECT_EQ(RecordedEvent::AGGREGATE_DROPPED, event12.type);
  EXPECT_EQ("keyspace1", event12.keyspace_name);
  EXPECT_EQ("aggregate1(varchar)", event12.target_name);
}

TEST_F(ControlConnectionUnitTest, EventDuringStartup) {
  Address address("127.0.0.1", PORT);

  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .up_event(address) // Send UP event during startup
      .system_local()
      .system_peers()
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  RecordingControlConnectionListener listener;

  bool is_connected;
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(address)), PROTOCOL_VERSION,
                           bind_callback(on_connection_connected, &is_connected)));
  connector->with_listener(&listener)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_TRUE(is_connected);
  ASSERT_GT(listener.events().size(), 1u);

  const RecordedEvent& event1 = listener.find_event(RecordedEvent::NODE_UP);
  EXPECT_EQ(RecordedEvent::NODE_UP, event1.type);
  EXPECT_EQ(address, event1.host->address());
}

TEST_F(ControlConnectionUnitTest, InvalidProtocol) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  ControlConnector::ControlConnectionError error_code(ControlConnector::CONTROL_CONNECTION_OK);
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                           0x7F, // Invalid protocol version
                           bind_callback(on_connection_error_code, &error_code)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(ControlConnector::CONTROL_CONNECTION_ERROR_CONNECTION, error_code);
  EXPECT_EQ(Connector::CONNECTION_ERROR_INVALID_PROTOCOL, connector->connection_error_code());
}

TEST_F(ControlConnectionUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  ControlConnector::ControlConnectionError error_code(ControlConnector::CONTROL_CONNECTION_OK);
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_connection_error_code, &error_code)));

  ControlConnectionSettings settings;
  settings.connection_settings.auth_provider.reset(new PlainTextAuthProvider("invalid", "invalid"));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(ControlConnector::CONTROL_CONNECTION_ERROR_CONNECTION, error_code);
  EXPECT_EQ(Connector::CONNECTION_ERROR_AUTH, connector->connection_error_code());
}

TEST_F(ControlConnectionUnitTest, InvalidSsl) {
  mockssandra::SimpleCluster cluster(simple());
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  ControlConnector::ControlConnectionError error_code(ControlConnector::CONTROL_CONNECTION_OK);
  ControlConnector::Ptr connector(
      new ControlConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_connection_error_code, &error_code)));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ControlConnectionSettings settings;
  settings.connection_settings.socket_settings.ssl_context = ssl_context;

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(ControlConnector::CONTROL_CONNECTION_ERROR_CONNECTION, error_code);
  EXPECT_EQ(Connector::CONNECTION_ERROR_SSL_VERIFY, connector->connection_error_code());
}
