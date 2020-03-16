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
#include "timer.hpp"
#include "wait_for_handler.hpp"

#include <ostream>

using namespace datastax::internal;
using namespace datastax::internal::core;

typedef Vector<WaitForHandler::WaitForError> Errors;

namespace std {

std::ostream& operator<<(std::ostream& os, const Errors& errors) {
  for (Errors::const_iterator it = errors.begin(); it != errors.end(); ++it) {
    if (it != errors.begin()) os << ", ";
    os << *it;
  }
  return os;
}

} // namespace std

class WaitForHandlerUnitTest : public LoopTest {
public:
  class TestWaitForHandler : public WaitForHandler {
  public:
    typedef SharedRefPtr<TestWaitForHandler> Ptr;

    TestWaitForHandler(uint64_t max_wait_time = 2000, uint64_t retry_wait_time = 200)
        : WaitForHandler(
              RequestHandler::Ptr(new RequestHandler(QueryRequest::Ptr(new QueryRequest("")),
                                                     ResponseFuture::Ptr(new ResponseFuture()))),
              Host::Ptr(new Host(Address())), Response::Ptr(), max_wait_time, retry_wait_time)
        , count_on_set_(0)
        , count_on_error_(0)
        , is_idempotent_(false) {}

    Ptr with_is_idempotent(bool is_idempotent) {
      is_idempotent_ = is_idempotent;
      return Ptr(this);
    }

    Ptr with_expected_error(WaitForHandler::WaitForError error) {
      expected_.push_back(error);
      return Ptr(this);
    }

    virtual RequestCallback::Ptr callback() {
      WaitforRequestVec requests;
      QueryRequest::Ptr table1_request(new QueryRequest("SELECT * FROM test.table1"));
      QueryRequest::Ptr table2_request(new QueryRequest("SELECT * FROM test.table2"));
      table1_request->set_is_idempotent(is_idempotent_);
      table2_request->set_is_idempotent(is_idempotent_);
      requests.push_back(WaitForRequest("table1", table1_request));
      requests.push_back(WaitForRequest("table2", table2_request));
      return WaitForHandler::callback(requests);
    }

    int count_on_set() const { return count_on_set_; }

  protected:
    virtual bool on_set(const ChainedRequestCallback::Ptr& callback) {
      EXPECT_EQ(0, count_on_error_); // Set shouldn't be called after an error
      count_on_set_++;
      return false; // Never complete
    }

    virtual void on_error(WaitForError code, const String& message) {
      ASSERT_NE(0, expected_.size());
      bool found_expected = false;
      for (Errors::const_iterator it = expected_.begin(), end = expected_.end();
           !found_expected && it != end; ++it) {
        if (*it == code) {
          found_expected = true;
        }
      }
      EXPECT_TRUE(found_expected) << "Expected error codes [ " << expected_
                                  << " ], but received error " << code;
      count_on_error_++;
    }

  private:
    Errors expected_;
    int count_on_set_;
    int count_on_error_;
    bool is_idempotent_;
  };

  void run(const TestWaitForHandler::Ptr& handler, uint64_t timeout = 0) {
    mockssandra::SimpleRequestHandlerBuilder builder;
    run(handler, builder, timeout);
  }

  void run(const TestWaitForHandler::Ptr& handler,
           mockssandra::SimpleRequestHandlerBuilder& builder, uint64_t timeout = 0) {
    mockssandra::SimpleCluster cluster(builder.build());
    ASSERT_EQ(cluster.start_all(), 0);

    handler_ = handler;
    timeout_ = timeout;

    Connector::Ptr connector(
        new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                      bind_callback(&WaitForHandlerUnitTest::on_connected, this)));
    connector->connect(loop());

    uv_run(loop(), UV_RUN_DEFAULT);
  }

private:
  struct CloseConnectionHandler {
    CloseConnectionHandler(const Connection::Ptr& connection)
        : connection(connection) {}

    void on_timeout(Timer* timer) {
      connection->close();
      delete this;
    }

    void start(uint64_t timeout) {
      timer.start(connection->loop(), timeout,
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
      ASSERT_TRUE(false) << "Connection had a failure: " << connector->error_message();
    }
  }

private:
  TestWaitForHandler::Ptr handler_;
  uint64_t timeout_;
};

TEST_F(WaitForHandlerUnitTest, CloseImmediatelyWhileWaiting) {
  run(TestWaitForHandler::Ptr(new TestWaitForHandler())
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_ERROR)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_CONNECTION_CLOSED));
}

TEST_F(WaitForHandlerUnitTest, CloseAfterTimeoutWhileWaiting) {
  run(TestWaitForHandler::Ptr(new TestWaitForHandler())
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_ERROR)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_CONNECTION_CLOSED),
      500);
}

TEST_F(WaitForHandlerUnitTest, CloseIdempotentImmediatelyWhileWaiting) {
  run(TestWaitForHandler::Ptr(new TestWaitForHandler())
          ->with_is_idempotent(true)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_TIMEOUT)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_CONNECTION_CLOSED));
}

TEST_F(WaitForHandlerUnitTest, CloseIdempotentAfterTimeoutWhileWaiting) {
  run(TestWaitForHandler::Ptr(new TestWaitForHandler())
          ->with_is_idempotent(true)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_REQUEST_TIMEOUT)
          ->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_CONNECTION_CLOSED),
      500);
}

TEST_F(WaitForHandlerUnitTest, EnsureOnSetNotCalledAfterTimeout) {
  TestWaitForHandler::Ptr handler(
      new TestWaitForHandler(1)); // Timeout handler before query returns

  // Make sure the query doesn't complete before the handler times out
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .system_local()
      .system_peers()
      .wait(200)
      .empty_rows_result(1);

  run(handler->with_expected_error(WaitForHandler::WAIT_FOR_ERROR_TIMEOUT), builder, 500);

  EXPECT_EQ(0, handler->count_on_set()); // Ensure on_set() never called
}
