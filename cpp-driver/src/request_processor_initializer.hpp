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

#ifndef __CASS_REQUEST_PROCESSOR_INITIALIZER_HPP_INCLUDED__
#define __CASS_REQUEST_PROCESSOR_INITIALIZER_HPP_INCLUDED__

#include "atomic.hpp"
#include "connection_pool_manager_initializer.hpp"
#include "ref_counted.hpp"
#include "request_processor.hpp"
#include "timestamp_generator.hpp"
#include "vector.hpp"

#include <uv.h>

namespace cass {

class Config;
class EventLoop;

class RequestProcessorInitializer : public RefCounted<RequestProcessorInitializer> {
public:
  typedef SharedRefPtr<RequestProcessorInitializer> Ptr;
  typedef Vector<Ptr> Vec;
  typedef void (*Callback)(RequestProcessorInitializer*);

  enum RequestProcessorError {
    REQUEST_PROCESSOR_OK,
    REQUEST_PROCESSOR_ERROR_KEYSPACE,
    REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE,
    REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT_ASYNC
  };

  RequestProcessorInitializer(const Host::Ptr& current_host,
                              int protocol_version,
                              const HostMap& hosts,
                              TokenMap* token_map,
                              MPMCQueue<RequestHandler*>* request_queue,
                              void* data, Callback callback);
  ~RequestProcessorInitializer();

  void initialize(EventLoop* event_loop);

  RequestProcessorInitializer* with_settings(const RequestProcessorSettings& setttings);
  RequestProcessorInitializer* with_listener(RequestProcessorListener* listener);
  RequestProcessorInitializer* with_keyspace(const String& keyspace);
  RequestProcessorInitializer* with_metrics(Metrics* metrics);
  RequestProcessorInitializer* with_random(Random* random);


  RequestProcessor::Ptr release_processor();

public:
  void* data() { return data_; }
  RequestProcessorError error_code() const { return error_code_; }
  const String& error_message() const { return error_message_; }

  bool is_ok() const { return error_code_ == REQUEST_PROCESSOR_OK; }

private:
  friend class RunInitializeProcessor;

private:
  void internal_intialize();

private:
  static void on_initialize(ConnectionPoolManagerInitializer* initializer);
  void handle_initialize(ConnectionPoolManagerInitializer* initializer);

private:
  uv_mutex_t mutex_;

  ConnectionPoolManagerInitializer::Ptr connection_pool_manager_initializer_;
  RequestProcessor::Ptr processor_;

  EventLoop* event_loop_;
  RequestProcessorListener* listener_;
  RequestProcessorSettings settings_;
  String keyspace_;
  Metrics* metrics_;
  Random* random_;

  const Host::Ptr current_host_;
  const int protocol_version_;
  HostMap hosts_;
  TokenMap* const token_map_;
  MPMCQueue<RequestHandler*>* const request_queue_;

  RequestProcessorError error_code_;
  String error_message_;

  void* data_;
  Callback callback_;

  Atomic<size_t> remaining_;
};

} // namespace cass

#endif
