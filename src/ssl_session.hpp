/*
  Copyright 2014 DataStax

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

#ifndef __CASS_SSL_SESSION_HPP_INCLUDED__
#define __CASS_SSL_SESSION_HPP_INCLUDED__

#include <openssl/ssl.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/pkcs12.h>

#include <deque>
#include <string>

#include "common.hpp"

#define BUFFER_SIZE 66560

// TODO(mpenick): Fix this and translate error from openssl
#define CASS_SSL_CHECK_ERROR(ssl, input)                         \
  {                                                              \
    int err = SSL_get_error(ssl, input);                         \
    if (err != SSL_ERROR_NONE && err != SSL_ERROR_WANT_READ) {   \
      char message[1024];                                        \
      ERR_error_string_n(err, message, sizeof(message));         \
      *error = Error(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_READ, \
                     std::string(message));                      \
      return false;                                              \
    }                                                            \
  }

namespace {

std::string get_ssl_error_string(SSL* ssl, int rc) {
  char message[1024];
  ERR_error_string_n(rc, message, sizeof(message));
  return std::string(message);
}

inline bool is_ssl_error(SSL* ssl, int rc) {
  int err = SSL_get_error(ssl, rc);
  return err != SSL_ERROR_NONE && err != SSL_ERROR_WANT_READ;
}

} // namepsace

namespace cass {

class SSLSession {

  SSL* ssl;

  BIO* ssl_bio;
  BIO* network_bio;
  BIO* internal_bio;

public:
  SSLSession(SSL_CTX* ctx)
      : ssl(SSL_new(ctx)) {}

  bool init() {
    if (!ssl) {
      return false;
    }

    if (!BIO_new_bio_pair(&internal_bio, BUFFER_SIZE, &network_bio,
                          BUFFER_SIZE)) {
      return false;
    }

    ssl_bio = BIO_new(BIO_f_ssl());
    if (!ssl_bio) {
      return false;
    }

    SSL_set_bio(ssl, internal_bio, internal_bio);
    BIO_set_ssl(ssl_bio, ssl, BIO_NOCLOSE);
    return true;
  }

  void shutdown() {
    SSL_shutdown(ssl);
    SSL_free(ssl);
  }

  void handshake(bool client) {
    if (client) {
      SSL_set_connect_state(ssl);
    } else {
      SSL_set_accept_state(ssl);
    }
    SSL_do_handshake(ssl);
  }

  bool handshake_done() { return SSL_is_init_finished(ssl); }

  char* ciphers(char* output, size_t size) {
    const SSL_CIPHER* sc = SSL_get_current_cipher(ssl);
    return SSL_CIPHER_description(sc, output, size);
  }

#define CHECK_SSL_ERROR(rc)                   \
  do {                                        \
    if (is_ssl_error(ssl, rc)) {              \
      *error = get_ssl_error_string(ssl, rc); \
      return false;                           \
    }                                         \
  } while (0)

  bool read_write(char* read_input, size_t read_input_size, size_t& read_size,
                  char** read_output, size_t& read_output_size,
                  char* write_input, size_t write_input_size,
                  char** write_output, size_t& write_output_size,
                  std::string* error) {
    if (write_input_size) {
      int write_status = BIO_write(ssl_bio, write_input, write_input_size);
      CHECK_SSL_ERROR(write_status);
    }

    int pending = BIO_ctrl_pending(ssl_bio);

    if (pending) {
      *read_output = new char[pending];
      read_output_size = BIO_read(ssl_bio, *read_output, pending);
      CHECK_SSL_ERROR(read_output_size);
    }

    if (read_input_size > 0) {
      if ((read_size = BIO_get_write_guarantee(network_bio))) {
        if (read_size > read_input_size) {
          read_size = read_input_size;
        }
        CHECK_SSL_ERROR(BIO_write(network_bio, read_input, read_size));
      }
    } else {
      read_size = 0;
    }

    write_output_size = BIO_ctrl_pending(network_bio);
    if (write_output_size) {
      *write_output = new char[write_output_size];
      CHECK_SSL_ERROR(BIO_read(network_bio, *write_output, write_output_size));
    }

    return true;
  }

#undef CHECK_SSL_ERROR
};

} // namespace cass

#endif
