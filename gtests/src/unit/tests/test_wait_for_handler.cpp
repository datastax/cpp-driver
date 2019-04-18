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

#include "connector.hpp"
#include "query_request.hpp"
#include "request_handler.hpp"
#include "wait_for_handler.hpp"
#include "timer.hpp"

using namespace cass;

class WaitForHandlerUnitTest : public LoopTest {
public:
  class TestWaitForHandler : public WaitForHandler {
  public:
    typedef SharedRefPtr<TestWaitForHandler> Ptr;

    TestWaitForHandler(uint64_t max_wait_time = 2000,
                       uint64_t retry_wait_time = 200)
      : WaitForHandler(RequestHandler::Ptr(
                         new RequestHandler(
                           QueryRequest::Ptr(new QueryRequest("")),
                           ResponseFuture::Ptr(new ResponseFuture()))),
                       Host::Ptr(new Host(Address())),
                       Response::Ptr(), max_wait_time, retry_wait_time) { }

    virtual RequestCallback::Ptr callback() = 0;

  private:
    virtual bool on_set(const ChainedRequestCallback::Ptr& callback) {
      return false; // Never complete
    }
  };

  class RegularQueryHandler : public TestWaitForHandler {
  public:
    virtual RequestCallback::Ptr callback() {
      WaitforRequestVec requests;
      requests.push_back(make_request("local", "SELECT * FROM system.local WHERE key='local'"));
      requests.push_back(make_request("peers", "SELECT * FROM system.peers"));
      return WaitForHandler::callback(requests);
    }

  private:
    virtual void on_error(WaitForError code, const String& message)  {
      EXPECT_TRUE(WAIT_FOR_ERROR_CONNECTION_CLOSED == code ||
                  WAIT_FOR_ERROR_REQUEST_ERROR == code);
    }
  };

  class IdempotentQueryHandler : public TestWaitForHandler {
  public:
    virtual RequestCallback::Ptr callback() {
      WaitforRequestVec requests;
      QueryRequest::Ptr local_request(new QueryRequest("SELECT * FROM system.local WHERE key='local'"));
      QueryRequest::Ptr peers_request(new QueryRequest("SELECT * FROM system.peers"));
      local_request->set_is_idempotent(true);
      peers_request->set_is_idempotent(true);
      requests.push_back(WaitForRequest("local", local_request));
      requests.push_back(WaitForRequest("peers", peers_request));
      return WaitForHandler::callback(requests);
    }

  private:
    virtual void on_error(WaitForError code, const String& message)  {
      EXPECT_TRUE(WAIT_FOR_ERROR_CONNECTION_CLOSED == code ||
                  WAIT_FOR_ERROR_REQUEST_TIMEOUT == code);
    }
  };

  void run(const TestWaitForHandler::Ptr& handler, uint64_t timeout = 0) {
    mockssandra::SimpleCluster cluster(simple());
    ASSERT_EQ(cluster.start_all(), 0);

    handler_ = handler;
    timeout_ = timeout;

    Connector::Ptr connector(
          new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                        PROTOCOL_VERSION,
                        bind_callback(&WaitForHandlerUnitTest::on_connected, this)));
    connector->connect(loop());

    uv_run(loop(), UV_RUN_DEFAULT);
  }

private:
  struct CloseConnectionHandler {
    CloseConnectionHandler(const Connection::Ptr& connection)
      : connection(connection) { }

    void on_timeout(Timer* timer) {
      connection->close();
      delete this;
    }

    void start(uint64_t timeout) {
      timer.start(connection->loop(),
                  timeout,
                  bind_callback(&CloseConnectionHandler::on_timeout, this));
    }

    Timer timer;
    Connection::Ptr connection;
  };

  static void close(const Connection::Ptr& connection, uint64_t timeout) {
    CloseConnectionHandler* handler(new CloseConnectionHandler(connection));
    handler->start(timeout);
  }

  void on_connected(Connector* connector) {
    if (connector->is_ok()) {
      Connection::Ptr connection(connector->release_connection());
      connection->write_and_flush(handler_->callback());
      if (timeout_ > 0) {
        close(connection, timeout_);
      } else {
        connection->close();
      }
    } else {
      ASSERT_TRUE(false) << "Connection had a failure: "
                         << connector->error_message();
    }
  }

private:
  TestWaitForHandler::Ptr handler_;
  uint64_t timeout_;
};

TEST_F(WaitForHandlerUnitTest, CloseImmediatelyWhileWaiting) {
  run(TestWaitForHandler::Ptr(new RegularQueryHandler()));
}

TEST_F(WaitForHandlerUnitTest, CloseAfterTimeoutWhileWaiting) {
  run(TestWaitForHandler::Ptr(new RegularQueryHandler()), 500);
}

TEST_F(WaitForHandlerUnitTest, CloseIdempotentImmediatelyWhileWaiting) {
  run(TestWaitForHandler::Ptr(new IdempotentQueryHandler()));
}

TEST_F(WaitForHandlerUnitTest, CloseIdempotentAfterTimeoutWhileWaiting) {
  run(TestWaitForHandler::Ptr(new IdempotentQueryHandler()), 500);
}
