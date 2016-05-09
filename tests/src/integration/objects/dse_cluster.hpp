/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __TEST_DSE_CLUSTER_HPP__
#define __TEST_DSE_CLUSTER_HPP__
#include "objects/cluster.hpp"

#include "dse.h"

namespace test {
namespace driver {

/**
 * Wrapped cluster object (builder) for DSE extras
 */
class DseCluster : public Cluster {
public:
  /**
   * Create the DSE cluster for the builder object
   */
  DseCluster()
    : Cluster() {}

  /**
   * Destroy the DSE cluster
   */
  virtual ~DseCluster() {};

  /**
   * Build/Create the DSE cluster
   *
   * @return DSE cluster object
   */
  static DseCluster build() {
    return DseCluster();
  }

  /**
   * Enable GSSAPI/SASL authentication
   *
   * @param service Name of the service
   * @param principal Principal for the server
   * @return DSE cluster object
   */
  DseCluster& with_gssapi_authenticator(const std::string& service,
    const std::string& principal) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_dse_gssapi_authenticator(get(),
      service.c_str(), principal.c_str()));
    return *this;
  }

  /**
   * Enable plain text authentication
   *
   * @param username Username to authenticate
   * @param password Password for username
   * @return DSE cluster object
   */
  DseCluster& with_plaintext_authenticator(const std::string& username,
    const std::string& password) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_dse_plaintext_authenticator(get(),
      username.c_str(), password.c_str()));
    return *this;
  }

};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_CLUSTER_HPP__
