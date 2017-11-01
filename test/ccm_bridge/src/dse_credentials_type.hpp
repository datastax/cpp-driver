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

#ifndef __CCM_DSE_CREDENTIALS_TYPE_HPP__
#define __CCM_DSE_CREDENTIALS_TYPE_HPP__

#include <set>
#include <string>

namespace CCM {

  /**
   * DSE credential stype indicating how authentication for DSE downloads is
   * performed through CCM
   */
  class DseCredentialsType {
  public:
    /**
     * Iterator for DSE credentials type constants
     */
    typedef std::set<DseCredentialsType>::iterator iterator;

    /**
     * Username/Password credentials type; DSE download process is authenticated
     * via plain text username and password
     */
    static const DseCredentialsType USERNAME_PASSWORD;
    /**
     * File credentials type; DSE download process is authenticated via the
     * CCM DSE credentials default file location (e.g. ~/.ccm/.dse.ini)
     */
    static const DseCredentialsType INI_FILE;

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
     * @return Display name of DSE credentials type
     */
    const std::string& to_string() const;

    /**
     * Less than (can be used for sorting)
     *
     * @param object Right hand side comparison object
     * @return True if LHS < RHS; false otherwise
     */
    bool operator<(const DseCredentialsType& object) const;
    /**
     * Equal to
     *
     * @param object Right hand side comparison object
     * @return True if LHS == RHS; false otherwise
     */
    bool operator==(const DseCredentialsType& object) const;
    /**
     * Equal to (case-incentive string comparison)
     *
     * @param object Right hand side comparison object
     * @return True if LHS == RHS; false otherwise
     */
    bool operator==(const std::string& object) const;

    /**
     * First item in the DSE credentials constants
     *
     * @return Iterator pointing to the first element in the set
     */
    static std::set<DseCredentialsType>::iterator begin();
    /**
     * Last item in the DSE credentials constants
     *
     * @return Iterator pointing to the last element in the set
     */
    static std::set<DseCredentialsType>::iterator end();

    /**
     * Default constructor to handle issues with static initialization of
     * constant DSE credentials types
     */
    DseCredentialsType();

  private:
    /**
     * DSE credentials type constants
     */
    static std::set<DseCredentialsType> constants_;
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
     * @param name Name for DSE credentials type
     * @param display_name Display name for DSE credentials type
     */
    DseCredentialsType(const std::string& name, int ordinal, const std::string& display_name);
    /**
     * Get the DSE credential type constants
     *
     * @return List of DSE credentials type constants
     */
    static const std::set<DseCredentialsType>& get_constants();
  };

}

#endif // __CCM_DSE_CREDENTIALS_TYPE_HPP__
