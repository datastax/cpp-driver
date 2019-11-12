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

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

Unit::OutagePlan::Action::Action(Type type, size_t node, uint64_t delay_ms)
    : type(type)
    , node(node)
    , delay_ms(delay_ms) {}

Unit::OutagePlan::OutagePlan(uv_loop_t* loop, mockssandra::SimpleCluster* cluster)
    : loop_(loop)
    , cluster_(cluster) {}

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

void Unit::OutagePlan::run(core::Future::Ptr future /*= core::Future::Ptr()*/) {
  if (future) future_ = future;
  next();
}

void Unit::OutagePlan::stop() { timer_.stop(); }

bool Unit::OutagePlan::is_done() { return (action_it_ == actions_.end()); }

void Unit::OutagePlan::next() {
  if (!is_done()) {
    if (action_it_->delay_ms > 0 && loop_ != NULL) {
      ASSERT_EQ(0, timer_.start(loop_, action_it_->delay_ms,
                                bind_callback(&OutagePlan::on_timeout, this)));
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

void Unit::OutagePlan::on_timeout(Timer* timer) { handle_timeout(); }

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
    : output_log_level_(CASS_LOG_DISABLED)
    , logging_criteria_count_(0) {
  Logger::set_log_level(CASS_LOG_TRACE);
  Logger::set_callback(on_log, this);
  uv_mutex_init(&mutex_);
}

Unit::~Unit() {
  Logger::set_log_level(CASS_LOG_DISABLED);
  Logger::set_callback(NULL, NULL);
  uv_mutex_destroy(&mutex_);
}

void Unit::set_output_log_level(CassLogLevel output_log_level) {
  output_log_level_ = output_log_level;
}

const mockssandra::RequestHandler* Unit::simple() {
  return mockssandra::SimpleRequestHandlerBuilder().build();
}

const mockssandra::RequestHandler* Unit::auth() {
  return mockssandra::AuthRequestHandlerBuilder().build();
}

ConnectionSettings Unit::use_ssl(mockssandra::Cluster* cluster, const String& cn /*= ""*/) {
  SslContext::Ptr ssl_context(SslContextFactory::create());

  String cert = cluster->use_ssl(cn);
  EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
  EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

  core::ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  return settings;
}

void Unit::add_logging_critera(const String& criteria,
                               CassLogLevel severity /*= CASS_LOG_LAST_ENTRY*/) {
  ScopedMutex l(&mutex_);
  logging_criteria_[severity].push_back(criteria);
}

int Unit::logging_criteria_count() {
  ScopedMutex l(&mutex_);
  return logging_criteria_count_;
}

void Unit::reset_logging_criteria() {
  ScopedMutex l(&mutex_);
  logging_criteria_.clear();
  logging_criteria_count_ = 0;
}

void Unit::on_log(const CassLogMessage* message, void* data) {
  Unit* instance = static_cast<Unit*>(data);

  if (message->severity <= instance->output_log_level_) {
    fprintf(stderr, "%u.%03u [%s] (%s:%d:%s): %s\n",
            static_cast<unsigned int>(message->time_ms / 1000),
            static_cast<unsigned int>(message->time_ms % 1000),
            cass_log_level_string(message->severity), message->file, message->line,
            message->function, message->message);
  }

  // Determine if the log message matches any of the criteria
  ScopedMutex l(&instance->mutex_);
  for (Map<CassLogLevel, Vector<String> >::iterator level_it = instance->logging_criteria_.begin(),
                                                    level_end = instance->logging_criteria_.end();
       level_it != level_end; ++level_it) {
    CassLogLevel level = level_it->first;
    Vector<String> criteria = level_it->second;
    for (Vector<String>::const_iterator it = criteria.begin(), end = criteria.end(); it != end;
         ++it) {
      if ((level == CASS_LOG_LAST_ENTRY || level == message->severity) &&
          strstr(message->message, it->c_str()) != NULL) {
        ++instance->logging_criteria_count_;
      }
    }
  }
}
