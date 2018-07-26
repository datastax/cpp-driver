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

#ifndef __CASS_CLUSTER_CONFIG_HPP_INCLUDED__
#define __CASS_CLUSTER_CONFIG_HPP_INCLUDED__

#include "config.hpp"
#include "external.hpp"

#include "uv.h"

namespace cass {

class ClusterConfig {
public:
  const Config& config() const { return config_; }
  Config& config() { return config_; }

private:
  Config config_;
};

} // namespace cass

EXTERNAL_TYPE(cass::ClusterConfig, CassCluster)

#endif
