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

#ifndef __CCM_DEPLOYMENT_TYPE_HPP__
#define __CCM_DEPLOYMENT_TYPE_HPP__

#include <set>
#include <string>

namespace CCM {

/**
 * Deployment type indicating how CCM commands should be executed
 */
class DeploymentType {
public:
  /**
   * Iterator for deployment type constants
   */
  typedef std::set<DeploymentType>::iterator iterator;

  /**
   * Local deployment type; commands are executed through local process
   */
  static const DeploymentType LOCAL;
#ifdef CASS_USE_LIBSSH2
  /**
   * Remote deployment type; commands are executed though libssh2
   */
  static const DeploymentType REMOTE;
#endif

  /**
   * Name of constant
   *
   * @return Name of constant
   */
  const std::string& name() const;
  /**
   * Ordinal of constant
   *
   * @return Ordinal of constant
   */
  short ordinal() const;
  /**
   * Get the display name
   *
   * @return Display name of deployment type
   */
  const std::string& to_string() const;

  /**
   * Less than (can be used for sorting)
   *
   * @param object Right hand side comparison object
   * @return True if LHS < RHS; false otherwise
   */
  bool operator<(const DeploymentType& object) const;
  /**
   * Equal to
   *
   * @param object Right hand side comparison object
   * @return True if LHS == RHS; false otherwise
   */
  bool operator==(const DeploymentType& object) const;
  /**
   * Equal to (case-incentive string comparison)
   *
   * @param object Right hand side comparison object
   * @return True if LHS == RHS; false otherwise
   */
  bool operator==(const std::string& object) const;

  /**
   * First item in the deployment constants
   *
   * @return Iterator pointing to the first element in the set
   */
  static std::set<DeploymentType>::iterator begin();
  /**
   * Last item in the deployment constants
   *
   * @return Iterator pointing to the last element in the set
   */
  static std::set<DeploymentType>::iterator end();

  /**
   * Default constructor to handle issues with static initialization of
   * constant deployment types
   */
  DeploymentType();

private:
  /**
   * Deployment type constants
   */
  static std::set<DeploymentType> constants_;
  /**
   * Name of constant
   */
  std::string name_;
  /**
   * Ordinal of constant
   */
  short ordinal_;
  /**
   * Display name for constant
   */
  std::string display_name_;

  /**
   * Constructor
   *
   * @param name Name for deployment type
   * @param display_name Display name for deployment type
   */
  DeploymentType(const std::string& name, int ordinal, const std::string& display_name);
  /**
   * Get the deployment type constants
   *
   * @return List of deployment type constants
   */
  static const std::set<DeploymentType>& get_constants();
};

} // namespace CCM

#endif // __CCM_DEPLOYMENT_TYPE_HPP__
