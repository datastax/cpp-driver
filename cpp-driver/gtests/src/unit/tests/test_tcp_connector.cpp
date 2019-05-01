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

#include "callback.hpp"
#include "tcp_connector.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

class TcpConnectorUnitTest : public LoopTest {
public:
  TcpConnectorUnitTest()
      : status_(TcpConnector::NEW) {}

  virtual void SetUp() {
    LoopTest::SetUp();
    uv_tcp_init(loop(), &tcp_);
    ASSERT_EQ(server_.listen(), 0);
  }

  virtual void TearDown() {
    uv_close(reinterpret_cast<uv_handle_t*>(&tcp_), NULL);
    LoopTest::TearDown();
    server_.close();
  }

  void close() { server_.close(); }

  void connect(const TcpConnector::Ptr& connector) {
    connector->connect(&tcp_, bind_callback(&TcpConnectorUnitTest::on_connect, this));
  }

  TcpConnector::Status status() const { return status_; }

private:
  void on_connect(TcpConnector* connector) { status_ = connector->status(); }

private:
  uv_tcp_t tcp_;
  TcpConnector::Status status_;
  mockssandra::SimpleEchoServer server_;
};

TEST_F(TcpConnectorUnitTest, Simple) {
  TcpConnector::Ptr connector(new TcpConnector(Address("127.0.0.1", 8888)));
  connect(connector);
  run_loop();
  EXPECT_EQ(TcpConnector::SUCCESS, status());
}

TEST_F(TcpConnectorUnitTest, Invalid) {
  TcpConnector::Ptr connector(new TcpConnector(Address("127.99.0.99", 8888)));
  connect(connector);
  run_loop();
  EXPECT_EQ(TcpConnector::FAILED_TO_CONNECT, status());
}

TEST_F(TcpConnectorUnitTest, InvalidPort) {
  TcpConnector::Ptr connector(new TcpConnector(Address("127.0.0.1", 9999)));
  connect(connector);
  run_loop();
  EXPECT_EQ(TcpConnector::FAILED_TO_CONNECT, status());
}

TEST_F(TcpConnectorUnitTest, Cancel) {
  TcpConnector::Ptr connector(new TcpConnector(Address("127.0.0.1", 8888)));
  connect(connector);
  connector->cancel();
  run_loop();
  EXPECT_EQ(TcpConnector::CANCELED, status());
}
