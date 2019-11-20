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

#ifndef TEST_SSL_HPP
#define TEST_SSL_HPP
#include "cassandra.h"

#include "objects/object_base.hpp"

#include <gtest/gtest.h>
#include <string>

namespace test { namespace driver {

/**
 * Wrapped SSL context object
 */
class Ssl : public Object<CassSsl, cass_ssl_free> {
public:
  /**
   * Create the default SSL object
   */
  Ssl()
      : Object<CassSsl, cass_ssl_free>(cass_ssl_new()) {}

  /**
   * Create the SSL object from the native driver object
   *
   * @param ssl Native driver object
   */
  Ssl(CassSsl* ssl)
      : Object<CassSsl, cass_ssl_free>(ssl) {}

  /**
   * Create the SSL object from a shared reference
   *
   * @param ssl Shared reference
   */
  Ssl(Ptr ssl)
      : Object<CassSsl, cass_ssl_free>(ssl) {}

  /**
   * Adds a trusted certificate. This is used to verify the peer's certificate.
   *
   * @param cert PEM formatted certificate string
   * @return Ssl object
   */
  Ssl& add_trusted_cert(const std::string& cert) {
    EXPECT_EQ(CASS_OK, cass_ssl_add_trusted_cert(get(), cert.c_str()));
    return *this;
  }

  /**
   * Sets verification performed on the peer's certificate.
   *
   * @param flags Verification flags
   * @return Ssl object
   */
  Ssl& with_verify_flags(int flags) {
    cass_ssl_set_verify_flags(get(), flags);
    return *this;
  }

  /**
   * Set client-side certificate chain. This is used to authenticate the client on the server-side.
   * This should contain the entire certificate chain starting with the certificate itself.
   *
   * @param cert PEM formatted certificate string
   * @return Ssl object
   */
  Ssl& with_cert(const std::string& cert) {
    EXPECT_EQ(CASS_OK, cass_ssl_set_cert(get(), cert.c_str()));
    return *this;
  }

  /**
   * Set client-side private key. This is used to authenticate the client on the server-side.
   *
   * @param key PEM formatted key string
   * @param password used to decrypt key
   * @return Ssl object
   */
  Ssl& with_private_key(const std::string& key, const std::string& password) {
    EXPECT_EQ(CASS_OK, cass_ssl_set_private_key(get(), key.c_str(), password.c_str()));
    return *this;
  }
};

}} // namespace test::driver

#endif
