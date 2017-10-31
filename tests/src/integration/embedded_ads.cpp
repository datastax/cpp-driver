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

#include "embedded_ads.hpp"

namespace test {

// Initialize static variables
uv_process_t EmbeddedADS::process_;
uv_mutex_t EmbeddedADS::mutex_;
std::string EmbeddedADS::configuration_directory_ = "";
std::string EmbeddedADS::configuration_file_ = "";
std::string EmbeddedADS::cassandra_keytab_file_ = "";
std::string EmbeddedADS::dse_keytab_file_ = "";
std::string EmbeddedADS::dseuser_keytab_file_ = "";
std::string EmbeddedADS::unknown_keytab_file_ = "";
std::string EmbeddedADS::bob_keytab_file_ = "";
std::string EmbeddedADS::bill_keytab_file_ = "";
std::string EmbeddedADS::charlie_keytab_file_ = "";
std::string EmbeddedADS::steve_keytab_file_ = "";
bool EmbeddedADS::is_initialized_ = false;

} // namespace test
