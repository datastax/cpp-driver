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

#ifndef __CCM_AUTHENTICATION_TYPE_HPP__
#define __CCM_AUTHENTICATION_TYPE_HPP__

#include <set>
#include <string>

namespace CCM {

  /**
   * Authentication type indicating how SSH authentication should be handled
   */
  class AuthenticationType {
  public:
    /**
     * Iterator for authentication type constants
     */
    typedef std::set<AuthenticationType>::iterator iterator;

    /**
     * Username/Password authentication type; SSH process is authenticated via
     * plain text username and password
     */
    static const AuthenticationType USERNAME_PASSWORD;
    /**
     * Public key authentication type; SSH process is authenticated via public
     * key
     */
    static const AuthenticationType PUBLIC_KEY;

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
     * @return Display name of authentication type
     */
    const std::string& to_string() const;

    /**
     * Less than (can be used for sorting)
     *
     * @param object Right hand side comparison object
     * @return True if LHS < RHS; false otherwise
     */
    bool operator<(const AuthenticationType& object) const;
    /**
     * Equal to
     *
     * @param object Right hand side comparison object
     * @return True if LHS == RHS; false otherwise
     */
    bool operator==(const AuthenticationType& object) const;
    /**
     * Equal to (case-incentive string comparison)
     *
     * @param object Right hand side comparison object
     * @return True if LHS == RHS; false otherwise
     */
    bool operator==(const std::string& object) const;

    /**
     * First item in the authentication constants
     *
     * @return Iterator pointing to the first element in the set
     */
    static std::set<AuthenticationType>::iterator begin();
    /**
     * Last item in the authentication constants
     *
     * @return Iterator pointing to the last element in the set
     */
    static std::set<AuthenticationType>::iterator end();

    /**
     * Default constructor to handle issues with static initialization of
     * constant authentication types
     */
    AuthenticationType();

  private:
    /**
     * Authentication type constants
     */
    static std::set<AuthenticationType> constants_;
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
     * @param name Name for authentication type
     * @param display_name Display name for authentication type
     */
    AuthenticationType(const std::string& name, int ordinal, const std::string& display_name);
    /**
     * Get the authentication type constants
     *
     * @return List of authentication type constants
     */
    static const std::set<AuthenticationType>& get_constants();
  };

}

#endif // __CCM_AUTHENTICATION_TYPE_HPP__
