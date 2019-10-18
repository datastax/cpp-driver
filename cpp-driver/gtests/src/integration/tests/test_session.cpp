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

#include "integration.hpp"

#include "cassandra.h"
#include "scoped_lock.hpp"

#include <queue>

#define EVENT_MAXIMUM_WAIT_TIME_MS 5000
#define EVENT_WAIT_FOR_NAP_MS 100

using namespace datastax::internal;

class SessionTest : public Integration {
public:
  typedef std::pair<CassHostListenerEvent, std::string> Event;
  typedef std::queue<Event> Events;

  SessionTest() { uv_mutex_init(&mutex_); }
  ~SessionTest() { uv_mutex_destroy(&mutex_); }

  void SetUp() {
    is_session_requested_ = false;
    Integration::SetUp();
  }

  void check_event(CassHostListenerEvent expected_event, short expected_node) {
    ScopedMutex l(&mutex_);
    std::stringstream expected_address;
    expected_address << ccm_->get_ip_prefix() << expected_node;
    Event event = events_.front();
    EXPECT_EQ(expected_event, event.first);
    EXPECT_EQ(expected_address.str().c_str(), event.second);
    events_.pop();
  }
  bool wait_for_event(size_t expected_count) {
    start_timer();
    while (elapsed_time() < EVENT_MAXIMUM_WAIT_TIME_MS && event_count() < expected_count) {
      msleep(EVENT_WAIT_FOR_NAP_MS);
    }
    return event_count() >= expected_count;
  }

protected:
  size_t event_count() {
    ScopedMutex l(&mutex_);
    return events_.size();
  }
  void add_event(CassHostListenerEvent event, CassInet inet) {
    ScopedMutex l(&mutex_);
    char address[CASS_INET_STRING_LENGTH];

    cass_inet_string(inet, address);
    if (event == CASS_HOST_LISTENER_EVENT_ADD) {
      TEST_LOG("Host " << address << " has been ADDED");
    } else if (event == CASS_HOST_LISTENER_EVENT_REMOVE) {
      TEST_LOG("Host " << address << " has been REMOVED");
    } else if (event == CASS_HOST_LISTENER_EVENT_UP) {
      TEST_LOG("Host " << address << " is UP");
    } else if (event == CASS_HOST_LISTENER_EVENT_DOWN) {
      TEST_LOG("Host " << address << " is DOWN");
    } else {
      TEST_LOG_ERROR("Invalid event [" << event << "] for " << address);
    }

    events_.push(Event(event, address));
  }
  static void on_host_listener(CassHostListenerEvent event, CassInet inet, void* data) {
    SessionTest* instance = static_cast<SessionTest*>(data);
    instance->add_event(event, inet);
  }

private:
  uv_mutex_t mutex_;
  Events events_;
};

CASSANDRA_INTEGRATION_TEST_F(SessionTest, MetricsWithoutConnecting) {
  CHECK_FAILURE;

  Session session;

  CassMetrics metrics;
  logger_.add_critera("Attempted to get metrics before connecting session object");
  cass_session_get_metrics(session.get(), &metrics);

  EXPECT_EQ(metrics.requests.min, 0u);
  EXPECT_EQ(metrics.requests.one_minute_rate, 0.0);
  EXPECT_EQ(logger_.count(), 1u);

  CassSpeculativeExecutionMetrics spec_ex_metrics;
  logger_.reset();
  logger_.add_critera(
      "Attempted to get speculative execution metrics before connecting session object");
  cass_session_get_speculative_execution_metrics(session.get(), &spec_ex_metrics);
  EXPECT_EQ(spec_ex_metrics.min, 0u);
  EXPECT_EQ(spec_ex_metrics.percentage, 0.0);
  EXPECT_EQ(logger_.count(), 1u);
}

CASSANDRA_INTEGRATION_TEST_F(SessionTest, ExternalHostListener) {
  CHECK_FAILURE;
  is_test_chaotic_ = true; // Destroy the cluster after the test completes

  Cluster cluster = default_cluster().with_load_balance_round_robin().with_host_listener_callback(
      on_host_listener, this);
  Session session = cluster.connect();

  // Initial node 1 events (add and up)
  ASSERT_TRUE(wait_for_event(2u));
  check_event(CASS_HOST_LISTENER_EVENT_ADD, 1);
  check_event(CASS_HOST_LISTENER_EVENT_UP, 1);

  // Bootstrap node 2 (add and up events)
  EXPECT_EQ(2u, ccm_->bootstrap_node());
  ASSERT_TRUE(wait_for_event(2u));
  check_event(CASS_HOST_LISTENER_EVENT_ADD, 2);
  check_event(CASS_HOST_LISTENER_EVENT_UP, 2);

  // Stop node 1 (down event)
  stop_node(1);
  ASSERT_TRUE(wait_for_event(1u));
  check_event(CASS_HOST_LISTENER_EVENT_DOWN, 1);

  // Restart node 1 (up event)
  ccm_->start_node(1);
  CCM::CassVersion cass_version = this->server_version_;
  if (!Options::is_cassandra()) {
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version();
  }
  if (cass_version >= "2.2") {
    ASSERT_TRUE(wait_for_event(1u));
  } else {
    ASSERT_TRUE(wait_for_event(3u)); // C* <= 2.1 fires remove and add event on restart
    check_event(CASS_HOST_LISTENER_EVENT_REMOVE, 1);
    check_event(CASS_HOST_LISTENER_EVENT_ADD, 1);
  }
  check_event(CASS_HOST_LISTENER_EVENT_UP, 1);

  // Decomission node 1 (down and remove events)
  force_decommission_node(1);
  ASSERT_TRUE(wait_for_event(1u));
  check_event(CASS_HOST_LISTENER_EVENT_DOWN, 1);
  check_event(CASS_HOST_LISTENER_EVENT_REMOVE, 1);

  session.close();
}
