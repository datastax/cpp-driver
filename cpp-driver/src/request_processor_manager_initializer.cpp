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

RequestProcessorManagerInitializer::RequestProcessorManagerInitializer(const Host::Ptr& connected_host,
                                                                       int protocol_version,
                                                                       const HostMap& hosts,
                                                                       const Callback& callback)
  : callback_(callback)
  , remaining_(0)
  , connected_host_(connected_host)
  , protocol_version_(protocol_version)
  , hosts_(hosts)
  , listener_(NULL)
  , metrics_(NULL)
  , random_(NULL)
  , token_map_(NULL) {
  uv_mutex_init(&lock_);
}

RequestProcessorManagerInitializer::~RequestProcessorManagerInitializer() {
  uv_mutex_destroy(&lock_);
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_settings(const RequestProcessorSettings& settings) {
  settings_ = settings;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_listener(RequestProcessorManagerListener* listener) {
  listener_ = listener;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

RequestProcessorManagerInitializer*RequestProcessorManagerInitializer::with_random(Random* random) {
  random_ = random;
  return this;
}

RequestProcessorManagerInitializer* RequestProcessorManagerInitializer::with_token_map(const TokenMap::Ptr& token_map) {
  token_map_ = token_map;
  return this;
}

void RequestProcessorManagerInitializer::initialize(EventLoopGroup* event_loop_group) {
  inc_ref();
  size_t thread_count_io = event_loop_group->size();
  remaining_.store(thread_count_io);
  manager_.reset(Memory::allocate<RequestProcessorManager>(listener_));
  for (size_t i = 0; i < thread_count_io; ++i) {
    RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(connected_host_,
                                                                                               protocol_version_,
                                                                                               hosts_,
                                                                                               token_map_,
                                                                                               bind_callback(&RequestProcessorManagerInitializer::on_initialize, this)));
    initializers_.push_back(initializer);
    initializer
        ->with_settings(settings_)
        ->with_listener(manager_.get())
        ->with_keyspace(keyspace_)
        ->with_metrics(metrics_)
        ->with_random(random_)
        ->initialize(event_loop_group->get(i));
  }
}

RequestProcessorInitializer::Vec RequestProcessorManagerInitializer::failures() const {
  ScopedMutex l(&lock_);
  return failures_;
}

RequestProcessorManager::Ptr RequestProcessorManagerInitializer::release_manager() {
  RequestProcessorManager::Ptr temp = manager_;
  manager_.reset();
  return temp;
}

void RequestProcessorManagerInitializer::on_initialize(RequestProcessorInitializer* initializer) {
  { // Lock
    ScopedMutex l(&lock_);

    if (initializer->is_ok()) {
      manager_->add_processor(initializer->release_processor(), RequestProcessorManager::Protected());
    } else {
      failures_.push_back(RequestProcessorInitializer::Ptr(initializer));
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
