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

void Unit::SetUp() {
  saved_log_level_ = cass::Logger::log_level();
  set_log_level(CASS_LOG_DISABLED);
}

void Unit::TearDown() {
  cass::Logger::set_log_level(saved_log_level_);
}

void Unit::set_log_level(CassLogLevel log_level) {
  cass::Logger::set_log_level(log_level);
}

const mockssandra::RequestHandler* Unit::simple() {
  return mockssandra::SimpleRequestHandlerBuilder().build();
}

const mockssandra::RequestHandler* Unit::auth() {
  return mockssandra::AuthRequestHandlerBuilder().build();
}

cass::ConnectionSettings Unit::use_ssl(mockssandra::Cluster* cluster) {
  cass::SslContext::Ptr ssl_context(cass::SslContextFactory::create());

  String cert = cluster->use_ssl();
  EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
  EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

  cass::ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  return settings;
}
