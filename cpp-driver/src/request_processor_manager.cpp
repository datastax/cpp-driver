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

#include "request_processor_manager.hpp"
#include "event_loop.hpp"

#include <algorithm>

namespace cass {

static inline bool least_busy_comp(const RequestProcessor::Ptr& a,
                                   const RequestProcessor::Ptr& b) {
  return a->request_count() < b->request_count();
}

class NopRequestProcessorManagerListener : public RequestProcessorManagerListener {
public:
  virtual void on_pool_up(const Address& address)  { }
  virtual void on_pool_down(const Address& address) { }
  virtual void on_pool_critical_error(const Address& address,
                                      Connector::ConnectionError code,
                                      const String& message) { }
  virtual void on_prepared_metadata_changed(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) { }
  virtual void on_close(RequestProcessorManager* manager) { }
};

static NopRequestProcessorManagerListener nop_request_processor_manager_listener__;

RequestProcessorManager::RequestProcessorManager(RequestProcessorManagerListener* listener)
  : current_(0)
  , processor_count_(0)
  , listener_(listener ? listener : &nop_request_processor_manager_listener__) {
  uv_mutex_init(&mutex_);
}

RequestProcessorManager::~RequestProcessorManager() {
  uv_mutex_destroy(&mutex_);
}

void RequestProcessorManager::close() {
  for (size_t i = 0; i < processors_.size(); ++i) {
    processors_[i]->close();
  }
}

void RequestProcessorManager::notify_host_add(const Host::Ptr& host) {
  for (size_t i = 0; i < processors_.size(); ++i) {
    processors_[i]->notify_host_add(host);
  }
}

void RequestProcessorManager::notify_host_remove(const Host::Ptr& host) {
  for (size_t i = 0; i < processors_.size(); ++i) {
    processors_[i]->notify_host_remove(host);
  }
}

void RequestProcessorManager::notify_token_map_changed(const TokenMap::Ptr& token_map) {
  for (size_t i = 0; i < processors_.size(); ++i) {
    processors_[i]->notify_token_map_changed(token_map);
  }
}

void RequestProcessorManager::process_request(const RequestHandler::Ptr& request_handler) {
  const RequestProcessor::Ptr& processor
      =  *std::min_element(processors_.begin(), processors_.end(),
                           least_busy_comp);
  processor->process_request(request_handler);
}

void RequestProcessorManager::add_processor(const RequestProcessor::Ptr& processor,
                                            Protected) {
  ScopedMutex l(&mutex_);
  processor_count_++;
  processors_.push_back(processor);
}

void RequestProcessorManager::on_close(RequestProcessor* processor) {
  ScopedMutex l(&mutex_);
  if (processor_count_ <= 0) {
    return;
  } else if (--processor_count_ == 0) {
    listener_->on_close(this);
  }
}

void RequestProcessorManager::on_pool_up(const Address& address) {
  listener_->on_pool_up(address);
}

void RequestProcessorManager::on_pool_down(const Address& address) {
  listener_->on_pool_down(address);
}

void RequestProcessorManager::on_pool_critical_error(const Address& address,
                                                     Connector::ConnectionError code,
                                                     const String& message) {
  listener_->on_pool_critical_error(address, code, message);
}

void RequestProcessorManager::on_keyspace_changed(const String& keyspace,
                                                  const KeyspaceChangedHandler::Ptr& handler) {
  for (size_t i = 0; i < processors_.size(); ++i) {
    processors_[i]->set_keyspace(keyspace, handler);
  }
}

void RequestProcessorManager::on_prepared_metadata_changed(const String& id,
                                                           const PreparedMetadata::Entry::Ptr& entry) {
  listener_->on_prepared_metadata_changed(id, entry);
}

} // namespace cass
