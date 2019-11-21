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

#include "dse_integration.hpp"

DseIntegration::DseIntegration()
    : Integration()
    , dse_session_() {}

void DseIntegration::SetUp() {
  // Call the parent setup class
  Integration::SetUp();

  // Create the DSE session object from the Cassandra session object
  dse_session_ = session_;
}

void DseIntegration::connect(dse::Cluster cluster) {
  // Call the parent connect function
  Integration::connect(cluster);
  dse_session_ = session_;
}

void DseIntegration::connect() {
  // Create the cluster configuration and establish the session connection
  cluster_ = default_cluster();
  dse_cluster_ = cluster_;
  connect(dse_cluster_);
}

Cluster DseIntegration::default_cluster(bool is_with_default_contact_points) {
  Cluster cluster = dse::Cluster::build()
                        .with_randomized_contact_points(is_randomized_contact_points_)
                        .with_schema_metadata(is_schema_metadata_);
  if (is_with_default_contact_points) {
    cluster.with_contact_points(contact_points_);
  }
  return cluster;
}
