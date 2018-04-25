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

#include "config.hpp"
#include "event_loop.hpp"
#include "request_processor_initializer.hpp"
#include "scoped_lock.hpp"

namespace cass {

class RunInitializeProcessor : public Task {
public:
  RunInitializeProcessor(const RequestProcessorInitializer::Ptr& initializer)
    : initializer_(initializer) { }

  virtual void run(EventLoop* event_loop) {
    initializer_->internal_intialize();
  }

private:
  RequestProcessorInitializer::Ptr initializer_;
};

RequestProcessorSettings::RequestProcessorSettings()
  : max_schema_wait_time_ms(10000)
  , prepare_on_all_hosts(true)
  , timestamp_generator(Memory::allocate<ServerSideTimestampGenerator>())
  , default_profile(Config().default_profile()){ }

RequestProcessorSettings::RequestProcessorSettings(const Config& config)
  : connection_pool_manager_settings(config)
  , max_schema_wait_time_ms(config.max_schema_wait_time_ms())
  , prepare_on_all_hosts(config.prepare_on_all_hosts())
  , timestamp_generator(config.timestamp_gen())
  , default_profile(config.default_profile())
  , profiles(config.profiles()) { }

RequestProcessorInitializer::RequestProcessorInitializer(const Host::Ptr& connected_host,
                                                         int protocol_version,
                                                         const HostMap& hosts,
                                                         TokenMap* token_map,
                                                         MPMCQueue<RequestHandler*>* request_queue,
                                                         void* data, Callback callback)
  : event_loop_(NULL)
  , listener_(NULL)
  , metrics_(NULL)
  , random_(NULL)
  , connected_host_(connected_host)
  , protocol_version_(protocol_version)
  , hosts_(hosts)
  , token_map_(token_map)
  , request_queue_(request_queue)
  , error_code_(REQUEST_PROCESSOR_OK)
  , data_(data)
  , callback_(callback) {
  uv_mutex_init(&mutex_);
}

RequestProcessorInitializer::~RequestProcessorInitializer() {
  uv_mutex_destroy(&mutex_);
}

void RequestProcessorInitializer::initialize(EventLoop* event_loop) {
  ScopedMutex l(&mutex_);
  event_loop_ = event_loop;
  event_loop_->add(Memory::allocate<RunInitializeProcessor>(Ptr(this)));
}

RequestProcessorInitializer* RequestProcessorInitializer::with_settings(const RequestProcessorSettings& settings) {
  settings_ = settings;
  return this;
}

RequestProcessorInitializer* RequestProcessorInitializer::with_listener(RequestProcessorListener* listener) {
  listener_ = listener;
  return this;
}

RequestProcessorInitializer*RequestProcessorInitializer::with_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
  return this;
}

RequestProcessorInitializer* RequestProcessorInitializer::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

RequestProcessorInitializer* RequestProcessorInitializer::with_random(Random* random) {
  random_ = random;
  return this;
}

RequestProcessor::Ptr RequestProcessorInitializer::release_processor() {
  RequestProcessor::Ptr temp(processor_);
  processor_.reset();
  return temp;
}

void RequestProcessorInitializer::internal_intialize() {
  inc_ref();
  connection_pool_manager_initializer_.reset(
        Memory::allocate<ConnectionPoolManagerInitializer>(protocol_version_,
                                                           this,
                                                           on_initialize));

    AddressVec addresses;
    addresses.reserve(addresses.size());
    for (HostMap::const_iterator it = hosts_.begin(),
         end = hosts_.end(); it != end; ++it) {
      addresses.push_back(it->first);
    }

  connection_pool_manager_initializer_
      ->with_settings(settings_.connection_pool_manager_settings)
      ->with_keyspace(keyspace_)
      ->with_metrics(metrics_)
      ->initialize(event_loop_->loop(), addresses);
}

void RequestProcessorInitializer::on_initialize(ConnectionPoolManagerInitializer* initializer) {
  RequestProcessorInitializer* processor_initializer = static_cast<RequestProcessorInitializer*>(initializer->data());
  processor_initializer->handle_initialize(initializer);
}

void RequestProcessorInitializer::handle_initialize(ConnectionPoolManagerInitializer* initializer) {
  bool is_keyspace_error = false;

  // Prune hosts
  const ConnectionPoolConnector::Vec& failures = initializer->failures();
  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(),
       end = failures.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr connector(*it);
    if (connector->is_keyspace_error()) {
      is_keyspace_error = true;
      break;
    } else {
      if (listener_) listener_->on_pool_down(connector->address());
      hosts_.erase(connector->address());
    }
  }

  // Handle errors and set hosts as up
  if (is_keyspace_error) {
    error_code_ = REQUEST_PROCESSOR_ERROR_KEYSPACE;
    error_message_ = "Keyspace '" + keyspace_ + "' does not exist";
  } else if (hosts_.empty()) {
    error_code_ = REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE;
    error_message_ = "Unable to connect to any hosts";
  } else {
    processor_.reset(Memory::allocate<RequestProcessor>(event_loop_,
                                                        initializer->release_manager(),
                                                        connected_host_,
                                                        hosts_,
                                                        token_map_,
                                                        listener_,
                                                        settings_,
                                                        random_,
                                                        request_queue_));

    int rc = processor_->init(RequestProcessor::Protected());
    if (rc != 0) {
      error_code_ = REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT_ASYNC;
      error_message_ = "Unable to initialize request processor async";
    } else {
      if (listener_) {
        for (HostMap::const_iterator it = hosts_.begin(),
             end = hosts_.end(); it != end; ++it) {
          it->second->set_up(); // TODO: We should do this in the cluster object?
          listener_->on_pool_up(it->first);
        }
      }
    }
  }

  callback_(this);
  dec_ref();
}

} // namespace cass
