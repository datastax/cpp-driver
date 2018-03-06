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

#include "mockssandra_test.hpp"

#include "logger.hpp"
#include "ssl.hpp"

namespace mockssandra {

SimpleClusterTest::SimpleClusterTest(size_t num_nodes,
                                     const RequestHandler* handler)
  : cluster_(handler != NULL ? handler
                             : mockssandra::SimpleRequestHandlerBuilder().build(),
             num_nodes)
  , saved_log_level_(CASS_LOG_DISABLED) { }

void SimpleClusterTest::SetUp() {
  saved_log_level_ = cass::Logger::log_level();
  set_log_level(CASS_LOG_DISABLED);
}

void SimpleClusterTest::TearDown() {
  stop_all();
  cass::Logger::set_log_level(saved_log_level_);
}

void SimpleClusterTest::set_log_level(CassLogLevel log_level) {
  cass::Logger::set_log_level(log_level);
}

cass::ConnectionSettings SimpleClusterTest::use_ssl() {
  cass::SslContext::Ptr ssl_context(cass::SslContextFactory::create());

  String cert = cluster_.use_ssl();
  EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
  EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

  cass::ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  return settings;
}

void SimpleClusterTest::start(size_t node) {
  if (cluster_.start(node) != 0) {
    cluster_.stop_all();
    ASSERT_TRUE(false) << "Unable to start node " << node;
  }
}

void SimpleClusterTest::stop(size_t node) {
  cluster_.stop(node);
}

void SimpleClusterTest::start_all() {
  ASSERT_EQ(cluster_.start_all(), 0);
}

void SimpleClusterTest::stop_all() {
  cluster_.stop_all();
}

void SimpleClusterTest::use_close_immediately() {
  cluster_.use_close_immediately();
}

} // namespace mockssandra
