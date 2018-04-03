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

#include "request_processor_manager_initializer.hpp"

namespace cass {

RequestProcessorManagerInitializer::RequestProcessorManagerInitializer(void* data,
                                                                       Callback callback)
  : data_(data)
  , callback_(callback)
  , listener_(NULL)
  , metrics_(NULL)
  , protocol_version_(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION)
  , request_queue_(NULL)
  , token_map_(NULL)
{
  uv_mutex_init(&lock_);
}

RequestProcessorManagerInitializer::~RequestProcessorManagerInitializer() {
  uv_mutex_destroy(&lock_);
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_cluster_config(const Config& config) {
  config_ = config;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_connect_keyspace(const String& connect_keyspace) {
  connect_keyspace_ = connect_keyspace;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_hosts(const Host::Ptr& connected_host,
                                                                                   const HostMap& hosts) {
  connected_host_ = connected_host;
  hosts_ = hosts;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_listener(RequestProcessorListener* listener) {
  listener_ = listener;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_protocol_version(int protocol_version) {
  protocol_version_ = protocol_version;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_request_queue(MPMCQueue<RequestHandler*>* request_queue) {
  request_queue_ = request_queue;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_token_map(TokenMap* token_map) {
  token_map_ = token_map;
  return this;
}

void RequestProcessorManagerInitializer::initialize() {
  inc_ref();
  remaining_.store(config_.thread_count_io());

  LOG_DEBUG("Creating %u IO worker threads",
            static_cast<unsigned int>(config_.thread_count_io()));
  manager_.reset(Memory::allocate<RequestProcessorManager>(config_.thread_count_io()));
  for (unsigned i = 0; i < config_.thread_count_io(); ++i) {
    RequestProcessor::Ptr request_processor(Memory::allocate<RequestProcessor>(config_,
                                                                               connect_keyspace_,
                                                                               listener_,
                                                                               metrics_,
                                                                               request_queue_,
                                                                               token_map_ ? token_map_->clone() : NULL,
                                                                               this,
                                                                               on_connect));
    request_processor->init(RequestProcessor::Protected());
    request_processor->run();
    request_processor->connect(connected_host_,
                               hosts_,
                               protocol_version_,
                               RequestProcessor::Protected());
  }
}

void* RequestProcessorManagerInitializer::data() {
  return data_;
}

cass::RequestProcessor::Vec RequestProcessorManagerInitializer::failures() const {
  ScopedMutex l(&lock_);
  return failures_;
}

RequestProcessorManager::Ptr RequestProcessorManagerInitializer::release_manager() {
  RequestProcessorManager::Ptr temp = manager_;
  manager_.reset();
  return temp;
}

void RequestProcessorManagerInitializer::on_connect(RequestProcessor* request_processor) {
  RequestProcessorManagerInitializer* initializer = static_cast<RequestProcessorManagerInitializer*>(request_processor->data());
  initializer->handle_connect(request_processor);
}

void RequestProcessorManagerInitializer::handle_connect(RequestProcessor* request_processor) {
  { // Lock
    ScopedMutex l(&lock_);

    if (request_processor->is_ok()) {
      manager_->add_request_processor(RequestProcessor::Ptr(request_processor),
                                      RequestProcessorManager::Protected());
    } else {
      request_processor->close();
      request_processor->join();
      failures_.push_back(RequestProcessor::Ptr(request_processor));
    }
  }

  if (remaining_.fetch_sub(1) - 1 == 0) {
    callback_(this);
    // If the request processor manager hasn't been released then close it
    if (manager_) manager_->close();
    dec_ref();
  }
}

} // namespace cass
