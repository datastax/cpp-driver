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

#ifndef DATASTAX_INTERNAL_CLOUD_SECURE_CONNECTION_CONFIG_HPP
#define DATASTAX_INTERNAL_CLOUD_SECURE_CONNECTION_CONFIG_HPP

#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class Config;

class CloudSecureConnectionConfig {
public:
  CloudSecureConnectionConfig();

  bool load(const String& filename, Config* config = NULL);
  bool is_loaded() const { return is_loaded_; }

  const String& username() const { return username_; }
  const String& password() const { return password_; }
  const String& host() const { return host_; }
  int port() const { return port_; }

  const String& ca_cert() const { return ca_cert_; }
  const String& cert() const { return cert_; }
  const String& key() const { return key_; }

private:
  bool is_loaded_;
  String username_;
  String password_;
  String host_;
  int port_;
  String ca_cert_;
  String cert_;
  String key_;
};

}}} // namespace datastax::internal::core

#endif
