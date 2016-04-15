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
#ifndef __DRIVER_OBJECT_CLUSTER_HPP__
#define __DRIVER_OBJECT_CLUSTER_HPP__
#include "cassandra.h"

#include "objects/future.hpp"
#include "objects/session.hpp"

#include "smart_ptr.hpp"

#include <string>

#include <gtest/gtest.h>

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassCluster
     */
    class ClusterDeleter {
    public:
      void operator()(CassCluster* cluster) {
        if (cluster) {
          cass_cluster_free(cluster);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassCluster, ClusterDeleter> ClusterPtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<CassCluster, ClusterDeleter> ClusterPtr;
      }
    }

    /**
     * Wrapped cluster object (builder)
     */
    class Cluster {
    public:
      /**
       * Create the cluster for the builder object
       */
      Cluster()
        : cluster_(cass_cluster_new()) {}

      /**
       * Destroy the cluster
       */
      virtual ~Cluster() {};

      /**
       * Build/Create the cluster
       *
       * @return Cluster object
       */
      static Cluster build() {
        return Cluster();
      }

      /**
       * Assign/Append the contact points; passing an empty string will clear
       * the contact points
       *
       * @param contact_points A comma delimited list of hosts (addresses or
       *                       names
       * @return Cluster object
       */
      Cluster& with_contact_points(const std::string& contact_points) {
        EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(cluster_.get(),
          contact_points.c_str()));
        return *this;
      }

      /**
       * Enable/Disable the use of hostname resolution
       *
       * This is useful for authentication (Kerberos) or encryption (SSL)
       * services that require a valid hostname for verification.
       *
       * NOTE: Not available if using libuv 0.10.x or earlier
       *
       * @param enable True if hostname resolution should be enable; false
       *               otherwise (default: false)
       * @return Cluster object
       */
      Cluster& with_hostname_resolution(cass_bool_t enable = cass_false) {
        EXPECT_EQ(CASS_OK, cass_cluster_set_use_hostname_resolution(cluster_.get(),
          enable));
        return *this;
      }

      /**
       * Assign the use of a particular binary protocol version; driver will
       * automatically downgrade to the lowest server supported version on
       * connection
       *
       * @param protocol_version Binary protocol version
       * @return Cluster object
       */
      Cluster& with_protocol_version(int protocol_version) {
        EXPECT_EQ(CASS_OK, cass_cluster_set_protocol_version(cluster_.get(),
          protocol_version));
        return *this;
      }

      /**
       * Enable/Disable the schema metadata
       *
       * If disabled this allows the driver to skip over retrieving and
       * updating schema metadata, but it also disables the usage of token-aware
       * routing and session->schema() will always return an empty object. This
       * can be useful for reducing the startup overhead of short-lived sessions
       *
       * @param enable True if schema metada should be enabled; false otherwise
       *               (default: true)
       * @return Cluster object
       */
      Cluster& with_schema_metadata(cass_bool_t enable = cass_true) {
        cass_cluster_set_use_schema(cluster_.get(), enable);
        return *this;
      }

      /**
       * Create a new session and establish a connection to the server;
       * synchronously
       *
       * @param keyspace Keyspace to use (default: None)
       * @return Session object
       * @throws Session::Exception If session could not be established
       */
      shared::SessionPtr connect(const std::string& keyspace = "") {
        return Session::connect(cluster_.get(), keyspace); 
      }

    protected:
      /**
       * Cluster driver reference object
       */
      shared::native::ClusterPtr cluster_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Cluster> ClusterPtr;
    }
    namespace shared {
      typedef SmartPtr<Cluster> ClusterPtr;
    }
  }
}

#endif // __DRIVER_OBJECT_CLUSTER_HPP__
