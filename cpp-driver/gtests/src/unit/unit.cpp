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

#include "scoped_lock.hpp"

Unit::OutagePlan::Action::Action(Type type, size_t node, uint64_t delay_ms)
  : type(type)
  , node(node)
  , delay_ms(delay_ms) { }

Unit::OutagePlan::OutagePlan(uv_loop_t* loop, mockssandra::SimpleCluster* cluster)
  : loop_(loop)
  , cluster_(cluster) { }

void Unit::OutagePlan::start_node(size_t node, uint64_t delay_ms /*= DEFAULT_OUTAGE_PLAN_DELAY*/) {
  actions_.push_back(Action(START_NODE, node, delay_ms));
  action_it_ = actions_.begin();
}

void Unit::OutagePlan::stop_node(size_t node, uint64_t delay_ms /*= DEFAULT_OUTAGE_PLAN_DELAY*/) {
  actions_.push_back(Action(STOP_NODE, node, delay_ms));
  action_it_ = actions_.begin();
}

void Unit::OutagePlan::add_node(size_t node, uint64_t delay_ms /*= DEFAULT_OUTAGE_PLAN_DELAY*/) {
  actions_.push_back(Action(ADD_NODE, node, delay_ms));
  action_it_ = actions_.begin();
}

void Unit::OutagePlan::remove_node(size_t node, uint64_t delay_ms /*= DEFAULT_OUTAGE_PLAN_DELAY*/) {
  actions_.push_back(Action(REMOVE_NODE, node, delay_ms));
  action_it_ = actions_.begin();
}

void Unit::OutagePlan::run(cass::Future::Ptr future /*= cass::Future::Ptr()*/) {
  if (future) future_ = future;
  next();
}

void Unit::OutagePlan::stop() {
  timer_.stop();
}

bool Unit::OutagePlan::is_done() {
  return (action_it_ == actions_.end());
}

void Unit::OutagePlan::next() {
  if (!is_done()) {
    if (action_it_->delay_ms > 0 && loop_ != NULL) {
      ASSERT_EQ(0, timer_.start(loop_, action_it_->delay_ms,
                cass::bind_callback(&OutagePlan::on_timeout, this)));
    } else {
      handle_timeout();
    }
  } else {
    if (future_) {
      stop();
      future_->set();
    }
  }
}

void Unit::OutagePlan::on_timeout(Timer* timer) {
  handle_timeout();
}

void Unit::OutagePlan::handle_timeout() {
  switch (action_it_->type) {
    case START_NODE:
      cluster_->start(action_it_->node);
      break;
    case STOP_NODE:
      cluster_->stop(action_it_->node);
      break;
    case ADD_NODE:
      cluster_->add(action_it_->node);
      break;
    case REMOVE_NODE:
      cluster_->remove(action_it_->node);
      break;
  };
  action_it_++;
  next();
}

Unit::Unit()
  : saved_log_level_(cass::Logger::log_level()) { }

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

cass::ConnectionSettings Unit::use_ssl(mockssandra::Cluster* cluster,
                                       const cass::String& cn /*= ""*/) {
  cass::SslContext::Ptr ssl_context(cass::SslContextFactory::create());

  String cert = cluster->use_ssl(cn);
  EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
  EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

  cass::ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  return settings;
}
