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

namespace cass {

RequestProcessorManagerSettings::RequestProcessorManagerSettings() { }

RequestProcessorManagerSettings::RequestProcessorManagerSettings(const Config& config)
  : connection_pool_manager_settings(config) { }

RequestProcessorManager::RequestProcessorManager()
  : current_(0) { }

void RequestProcessorManager::close() {
  internal_close();
}

void RequestProcessorManager::close_handles() {
  internal_close_handles();
}

void RequestProcessorManager::keyspace_update(const String& keyspace) {
  internal_keyspace_update(keyspace);
}

void RequestProcessorManager::notify_host_add_async(const Host::Ptr& host) {
  internal_notify_host_add_async(host);
}

void RequestProcessorManager::notify_host_remove_async(const Host::Ptr& host) {
  internal_notify_host_add_async(host);
}

void RequestProcessorManager::notify_token_map_update_async(const TokenMap* token_map) {
  internal_notify_token_map_update_async(token_map);
}

void RequestProcessorManager::notify_request_async() {
  internal_notify_request_async();
}
void RequestProcessorManager::add_request_processor(const RequestProcessor::Ptr& request_processor,
                                                    Protected) {
  internal_add_request_processor(request_processor);
}

void RequestProcessorManager::internal_add_request_processor(const RequestProcessor::Ptr& request_processor) {
  request_processors_.push_back(request_processor);
}

void RequestProcessorManager::internal_close() {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->close();
  }
}

void RequestProcessorManager::internal_close_handles() {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->close_handles();
  }
}

void RequestProcessorManager::internal_notify_host_add_async(const Host::Ptr& host) {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->notify_host_add_async(host);
  }
}

void RequestProcessorManager::internal_notify_host_remove_async(const Host::Ptr& host) {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->notify_host_remove_async(host);
  }
}

void RequestProcessorManager::internal_keyspace_update(const String& keyspace) {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->keyspace_update(keyspace);
  }
}

void RequestProcessorManager::internal_notify_token_map_update_async(const TokenMap* token_map) {
  for (size_t i = 0; i < request_processors_.size(); ++i) {
    request_processors_[i]->notify_token_map_update_async(token_map->clone());
  }
}

void RequestProcessorManager::internal_notify_request_async() {
  size_t index = current_.fetch_add(1) % request_processors_.size();
  request_processors_[index]->notify_request_async();
}

} // namespace cass
