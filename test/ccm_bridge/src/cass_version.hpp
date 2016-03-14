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
#ifndef __CCM_CASS_VERSION_HPP__
#define __CCM_CASS_VERSION_HPP__

#include <algorithm>
#include <sstream>
#include <string>

namespace CCM {

  /**
   * Cassandra release version number
   */
  class CassVersion {
  public:
    /**
     * Major portion of version number
     */
    unsigned short major_version;
    /**
     * Minor portion of version number
     */
    unsigned short minor_version;
    /**
     * Patch portion of version number
     */
    unsigned short patch_version;
    /**
     * Extra portion of version number
     */
    std::string extra;

    /**
     * Create the CassVersion from a human readable string
     *
     * @param version_string String representation to convert
     */
    CassVersion(const std::string& version_string)
      : major_version(0)
      , minor_version(0)
      , patch_version(0)
      , extra("") {
      from_string(version_string);
    }

    int compare(const CassVersion& rhs) {
      if (major_version < rhs.major_version) return -1;
      if (major_version > rhs.major_version) return  1;

      if (minor_version < rhs.minor_version) return -1;
      if (minor_version > rhs.minor_version) return  1;

      if (patch_version < rhs.patch_version) return -1;
      if (patch_version > rhs.patch_version) return  1;

      return 0;
    }

    /**
     * Equal comparison operator overload
     *
     * Determine if a Cassandra version is equal to another Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra version for compare
     * @return True if LHS == RHS; false otherwise
     */
    bool operator ==(const CassVersion& rhs) {
      return compare(rhs) == 0;
    }

    /**
     * Equal comparison operator overload
     *
     * Determine if a Cassandra version is equal to another Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS == RHS; false otherwise
     */
    bool operator ==(const std::string& version) {
      // Check version properties for equality (except extra property)
      return compare(CassVersion(version)) == 0;
    }

    /**
     * Not equal comparison operator overload
     *
     * Determine if a Cassandra version is not equal to another Cassandra
     * version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra version for compare
     * @return True if LHS != RHS; false otherwise
     */
    bool operator !=(const CassVersion& rhs) {
      return compare(rhs) != 0;
    }

    /**
     * Not equal comparison operator overload
     *
     * Determine if a Cassandra version is not equal to another Cassandra
     * version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS != RHS; false otherwise
     */
    bool operator !=(const std::string& version) {
      return compare(CassVersion(version)) != 0;
    }

    /**
     * Less-than comparison operator overload
     *
     * Determine if a Cassandra version is less-than another Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra Version to compare
     * @return True if LHS < RHS; false otherwise
     */
    bool operator <(const CassVersion& rhs) {
      return compare(rhs) < 0;
    }

    /**
     * Less-than comparison operator overload
     *
     * Determine if a Cassandra version is less-than another Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS < RHS; false otherwise
     */
    bool operator <(const std::string& version) {
      return compare(CassVersion(version)) < 0;
    }

    /**
     * Greater-than comparison operator overload
     *
     * Determine if a Cassandra version is greater-than another Cassandra
     * version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra Version to compare
     * @return True if LHS > RHS; false otherwise
     */
    bool operator >(const CassVersion& rhs) {
      return compare(rhs) > 0;
    }

    /**
     * Greater-than comparison operator overload
     *
     * Determine if a Cassandra version is greater-than another Cassandra
     * version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS > RHS; false otherwise
     */
    bool operator >(const std::string& version) {
      return compare(CassVersion(version)) > 0;
    }

    /**
     * Less-than or equal to comparison operator overload
     *
     * Determine if a Cassandra version is less-than or equal to another
     * Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra Version to compare
     * @return True if LHS <= RHS; false otherwise
     */
    bool operator <=(const CassVersion& rhs) {
      return compare(rhs) <= 0;
    }

    /**
     * Less-than or equal to comparison operator overload
     *
     * Determine if a Cassandra version is less-than or equal to another
     * Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS <= RHS; false otherwise
     */
    bool operator <=(const std::string& version) {
      return compare(CassVersion(version)) <= 0;
    }

    /**
     * Greater-than or equal to comparison operator overload
     *
     * Determine if a Cassandra version is greater-than or equal to another
     * Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param rhs Cassandra Version to compare
     * @return True if LHS >= RHS; false otherwise
     */
    bool operator >=(const CassVersion& rhs) {
      return compare(rhs) >= 0;
    }

    /**
     * Greater-than or equal to comparison operator overload
     *
     * Determine if a Cassandra version is greater-than or equal to another
     * Cassandra version
     *
     * NOTE: Extra property is not involved in comparison
     *
     * @param version Cassandra string version for compare
     * @return True if LHS >= RHS; false otherwise
     */
    bool operator >=(const std::string& version) {
      return compare(CassVersion(version)) >= 0;
    }

    /**
     * Convert the version into a human readable string
     *
     * @param is_extra_requested True if extra field should be added to version
     *                           string (iff !empty); false to disregard extra
     *                           field (default: true)
     */
    std::string to_string(bool is_extra_requested = true) {
      std::stringstream version_string;
      // Determine if tick-tock release should be handled
      if (*this > "3.0.0" && patch_version == 0) {
        version_string << major_version << "." << minor_version;
      } else {
        version_string << major_version << "." << minor_version << "." << patch_version;
      }

      // Determine if the extra version information should be added
      if (is_extra_requested && !extra.empty()) {
        version_string << "-" << extra;
      }
      return version_string.str();
    }

  private:
    /**
     * Convert the version from human readable string to version parameters
     *
     * @param version_string String representation to convert
     */
    void from_string(const std::string& version_string) {
      // Clean up the string for tokens
      std::string version(version_string);
      std::replace(version.begin(), version.end(), '.', ' ');
      std::size_t found = version.find("-");
      if (found != std::string::npos) {
        version.replace(found, 1, " ");
      }

      // Convert to tokens and assign version parameters
      std::istringstream tokens(version);
      if (tokens >> major_version) {
        if (tokens >> minor_version) {
          if (tokens >> patch_version) {
            tokens >> extra;
          }
        }
      }
    }
  };

}

#endif // __CCM_CASS_VERSION_HPP__
