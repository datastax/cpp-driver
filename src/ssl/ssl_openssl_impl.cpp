/*
  Copyright (c) 2014-2015 DataStax

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

#include "ssl.hpp"

#include "common.hpp"
#include "logger.hpp"
#include "ssl/ring_buffer_bio.hpp"
#include "string_ref.hpp"

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#include <string.h>

#define DEBUG_SSL 0

namespace cass {

#if DEBUG_SSL
#define SSL_PRINT_INFO(ssl, w, flag, msg) do { \
    if (w & flag) {                             \
      fprintf(stderr, "%s - %s - %s\n",        \
              msg,                             \
              SSL_state_string(ssl),           \
              SSL_state_string_long(ssl));     \
    }                                          \
 } while (0);

static void ssl_info_callback(const SSL* ssl, int where, int ret) {
  if (ret == 0) {
    fprintf(stderr, "ssl_info_callback, error occurred.\n");
    return;
  }
  SSL_PRINT_INFO(ssl, where, SSL_CB_LOOP, "LOOP");
  SSL_PRINT_INFO(ssl, where, SSL_CB_EXIT, "EXIT");
  SSL_PRINT_INFO(ssl, where, SSL_CB_READ, "READ");
  SSL_PRINT_INFO(ssl, where, SSL_CB_WRITE, "WRITE");
  SSL_PRINT_INFO(ssl, where, SSL_CB_ALERT, "ALERT");
  SSL_PRINT_INFO(ssl, where, SSL_CB_HANDSHAKE_DONE, "HANDSHAKE DONE");
}

#undef SSL_PRINT_INFO
#endif

static int ssl_no_verify_callback(int ok, X509_STORE_CTX* store) {
  // Verification happens after in SslSession::verify()
  // via SSL_get_verify_result().
  return 1;
}

static void ssl_log_errors(const char* context) {
  const char* data;
  int flags;
  int err;
  while ((err = ERR_get_error_line_data(NULL, NULL, &data, &flags)) != 0) {
    char buf[256];
    ERR_error_string_n(err, buf, sizeof(buf));
    LOG_ERROR("%s: %s:%s", context, buf, (flags & ERR_TXT_STRING) ? data : "");
  }
  ERR_print_errors_fp(stderr);
}

static std::string ssl_error_string(long err) {
  char buf[256];
  ERR_error_string_n(err, buf, sizeof(buf));
  return std::string(buf);
}

static int pem_password_callback(char* buf, int size, int rwflag, void* u) {
  if (u == NULL) return 0;

  int len = strlen(static_cast<const char*>(u));
  if (len == 0) return 0;

  int to_copy = size;
  if (len < to_copy) {
    to_copy = len;
  }
  memcpy(buf, u, to_copy);

  return len;
}

static uv_rwlock_t* crypto_locks;

static void crypto_locking_callback(int mode, int n, const char* file, int line) {
  if (mode & CRYPTO_LOCK) {
    if (mode & CRYPTO_READ) {
      uv_rwlock_rdlock(crypto_locks + n);
    } else {
      uv_rwlock_wrlock(crypto_locks + n);
    }
  } else {
    if (mode & CRYPTO_READ) {
      uv_rwlock_rdunlock(crypto_locks + n);
    } else {
      uv_rwlock_wrunlock(crypto_locks + n);
    }
  }
}

static unsigned long crypto_id_callback() {
#if UV_VERSION_MAJOR == 0
  return uv_thread_self();
#elif defined(WIN32) || defined(_WIN32) 
  return static_cast<unsigned long>(GetCurrentThreadId());
#else
  return copy_cast<uv_thread_t, unsigned long>(uv_thread_self());
#endif
}

// Implementation taken from OpenSSL's SSL_CTX_use_certificate_chain_file()
// (https://github.com/openssl/openssl/blob/OpenSSL_0_9_8-stable/ssl/ssl_rsa.c#L705).
// Modified to be used for in-memory certificate chains and formatting.
static int SSL_CTX_use_certificate_chain_bio(SSL_CTX* ctx, BIO* in) {
  int ret = 0;
  X509* x = NULL;

  x = PEM_read_bio_X509_AUX(in, NULL, pem_password_callback, NULL);
  if (x == NULL) {
    SSLerr(SSL_F_SSL_CTX_USE_CERTIFICATE_CHAIN_FILE,ERR_R_PEM_LIB);
    goto end;
  }

  ret = SSL_CTX_use_certificate(ctx, x);

  if (ERR_peek_error() != 0) {
    // Key/certificate mismatch doesn't imply ret==0 ...
    ret = 0;
  }

  if (ret) {
    // If we could set up our certificate, now proceed to
    // the CA certificates.

    X509 *ca;
    int r;
    unsigned long err;

    if (ctx->extra_certs != NULL) {
      sk_X509_pop_free(ctx->extra_certs, X509_free);
      ctx->extra_certs = NULL;
    }

    while ((ca = PEM_read_bio_X509(in, NULL, pem_password_callback, NULL)) != NULL) {
      r = SSL_CTX_add_extra_chain_cert(ctx, ca);
      if (!r) {
        X509_free(ca);
        ret = 0;
        goto end;
      }
      // Note that we must not free r if it was successfully
      // added to the chain (while we must free the main
      // certificate, since its reference count is increased
      // by SSL_CTX_use_certificate).
    }
    // When the while loop ends, it's usually just EOF.
    err = ERR_peek_last_error();
    if (ERR_GET_LIB(err) == ERR_LIB_PEM && ERR_GET_REASON(err) == PEM_R_NO_START_LINE) {
      ERR_clear_error();
    } else {
      // Some real error
      ret = 0;
    }
  }

end:
  if (x != NULL) X509_free(x);
  return ret;
}

static X509* load_cert(const char* cert, size_t cert_size) {
  BIO* bio = BIO_new_mem_buf(const_cast<char*>(cert), cert_size);
  if (bio == NULL) {
    return NULL;
  }

  X509* x509 = PEM_read_bio_X509(bio, NULL, pem_password_callback, NULL);
  if (x509 == NULL) {
    ssl_log_errors("Unable to load certificate");
  }

  BIO_free_all(bio);

  return x509;
}

static EVP_PKEY* load_key(const char* key,
                          size_t key_size,
                          const char* password) {
  BIO* bio = BIO_new_mem_buf(const_cast<char*>(key), key_size);
  if (bio == NULL) {
    return NULL;
  }

  EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio,
                                           NULL,
                                           pem_password_callback,
                                           const_cast<char*>(password));
  if (pkey == NULL) {
    ssl_log_errors("Unable to load private key");
  }

  BIO_free_all(bio);

  return pkey;
}

class OpenSslVerifyIdentity {
public:
  enum Result {
    INVALID_CERT,
    MATCH,
    NO_MATCH,
    NO_SAN_PRESENT
  };

  static Result match(X509* cert, const Address& addr) {
    Result result = match_subject_alt_names(cert, addr);
    if (result == NO_SAN_PRESENT) {
      result = match_common_name(cert, addr);
    }
    return result;
  }

private:
  static Result match_common_name(X509* cert, const Address& addr) {
    std::string addr_str = addr.to_string();

    X509_NAME* name = X509_get_subject_name(cert);
    if (name == NULL) {
      return INVALID_CERT;
    }

    int i = -1;
    while ((i = X509_NAME_get_index_by_NID(name, NID_commonName, i)) > 0) {
      X509_NAME_ENTRY* name_entry = X509_NAME_get_entry(name, i);
      if (name_entry == NULL) {
        return INVALID_CERT;
      }

      ASN1_STRING* str = X509_NAME_ENTRY_get_data(name_entry);
      StringRef common_name(reinterpret_cast<char*>(ASN1_STRING_data(str)), ASN1_STRING_length(str));
      if (iequals(common_name, addr_str)) {
        return MATCH;
      }
    }

    return NO_MATCH;
  }

  static Result match_subject_alt_names(X509* cert, const Address& addr) {
    char addr_buf[16];
    size_t addr_buf_size;
    if (addr.family() == AF_INET) {
      addr_buf_size = 4;
      memcpy(addr_buf, &addr.addr_in()->sin_addr.s_addr, addr_buf_size);
    } else {
      addr_buf_size = 16;
      memcpy(addr_buf, &addr.addr_in6()->sin6_addr, addr_buf_size);
    }

    STACK_OF(GENERAL_NAME)* names
      = static_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(cert, NID_subject_alt_name, NULL, NULL));
    if (names == NULL) {
      return NO_SAN_PRESENT;
    }

    for (int i = 0; i < sk_GENERAL_NAME_num(names); ++i) {
      GENERAL_NAME* name = sk_GENERAL_NAME_value(names, i);

      if (name->type == GEN_IPADD){
        ASN1_STRING* str = name->d.iPAddress;
        unsigned char* ip = ASN1_STRING_data(str);
        int ip_len = ASN1_STRING_length(str);
        if (ip_len != 4 && ip_len != 16) {
          return INVALID_CERT;
        }
        if (static_cast<size_t>(ip_len) == addr_buf_size &&
            memcmp(ip, addr_buf, addr_buf_size) == 0) {
          return MATCH;
        }
      }
    }
    sk_GENERAL_NAME_pop_free(names, GENERAL_NAME_free);

    return NO_MATCH;
  }
};

OpenSslSession::OpenSslSession(const Address& address,
                               int flags,
                               SSL_CTX* ssl_ctx)
  : SslSession(address, flags)
  , ssl_(SSL_new(ssl_ctx))
  , incoming_bio_(rb::RingBufferBio::create(&incoming_))
  , outgoing_bio_(rb::RingBufferBio::create(&outgoing_)) {
  SSL_set_bio(ssl_, incoming_bio_, outgoing_bio_);
  SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, ssl_no_verify_callback);
#if DEBUG_SSL
  SSL_CTX_set_info_callback(ssl_ctx, ssl_info_callback);
#endif
  SSL_set_connect_state(ssl_);
}

OpenSslSession::~OpenSslSession() {
  SSL_free(ssl_);
}

void OpenSslSession::do_handshake() {
  int rc = SSL_connect(ssl_);
  if (rc <= 0) {
    check_error(rc);
  }
}

void OpenSslSession::verify() {
  if (!verify_flags_) return;

  X509* peer_cert = SSL_get_peer_certificate(ssl_);
  if (peer_cert == NULL) {
    error_code_ = CASS_ERROR_SSL_NO_PEER_CERT;
    error_message_ = "No peer certificate found";
    return;
  }

  if (verify_flags_ & CASS_SSL_VERIFY_PEER_CERT) {
    int rc = SSL_get_verify_result(ssl_);
    if (rc != X509_V_OK) {
      error_code_ = CASS_ERROR_SSL_INVALID_PEER_CERT;
      error_message_ = X509_verify_cert_error_string(rc);
      X509_free(peer_cert);
      return;
    }
  }

  if (verify_flags_ & CASS_SSL_VERIFY_PEER_IDENTITY) {
    // We can only match IP address because that's what
    // Cassandra has in system local/peers tables

    switch (OpenSslVerifyIdentity::match(peer_cert, addr_)) {
      case OpenSslVerifyIdentity::MATCH:
        // Success
        break;

      case OpenSslVerifyIdentity::INVALID_CERT:
        error_code_ = CASS_ERROR_SSL_INVALID_PEER_CERT;
        error_message_ = "Peer certificate has malformed name field(s)";
        X509_free(peer_cert);
        return;

      default:
        error_code_ = CASS_ERROR_SSL_IDENTITY_MISMATCH;
        error_message_ = "Peer certificate subject name does not match";
        X509_free(peer_cert);
        return;
    }
  }

  X509_free(peer_cert);
}

int OpenSslSession::encrypt(const char* buf, size_t size) {
  int rc = SSL_write(ssl_, buf, size);
  if (rc <= 0) {
    check_error(rc);
  }
  return rc;
}

int OpenSslSession::decrypt(char* buf, size_t size)  {
  int rc = SSL_read(ssl_, buf, size);
  if (rc <= 0) {
    check_error(rc);
  }
  return rc;
}

bool OpenSslSession::check_error(int rc) {
  int err = SSL_get_error(ssl_, rc);
  if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_NONE) {
    error_message_ = ssl_error_string(err);
    return true;
  }
  return false;
}

OpenSslContext::OpenSslContext()
  : ssl_ctx_(SSL_CTX_new(SSLv23_client_method()))
  , trusted_store_(X509_STORE_new()) {
  SSL_CTX_set_cert_store(ssl_ctx_, trusted_store_);
}

OpenSslContext::~OpenSslContext() {
  SSL_CTX_free(ssl_ctx_);
}

SslSession* OpenSslContext::create_session(const Address& address ) {
  return new OpenSslSession(address, verify_flags_, ssl_ctx_);
}

CassError OpenSslContext::add_trusted_cert(const char* cert,
                                           size_t cert_length) {
  X509* x509 = load_cert(cert, cert_length);
  if (x509 == NULL) {
    return CASS_ERROR_SSL_INVALID_CERT;
  }

  X509_STORE_add_cert(trusted_store_, x509);
  X509_free(x509);

  return CASS_OK;
}

CassError OpenSslContext::set_cert(const char* cert,
                                   size_t cert_length) {
  BIO* bio = BIO_new_mem_buf(const_cast<char*>(cert), cert_length);
  if (bio == NULL) {
    return CASS_ERROR_SSL_INVALID_CERT;
  }

  int rc = SSL_CTX_use_certificate_chain_bio(ssl_ctx_, bio);

  BIO_free_all(bio);

  if (!rc) {
    ssl_log_errors("Unable to load certificate chain");
    return CASS_ERROR_SSL_INVALID_CERT;
  }

  return CASS_OK;
}

CassError OpenSslContext::set_private_key(const char* key,
                                          size_t key_length,
                                          const char* password,
                                          size_t password_length) {
  // TODO: Password buffer
  EVP_PKEY* pkey = load_key(key, key_length, password);
  if (pkey == NULL) {
    return CASS_ERROR_SSL_INVALID_PRIVATE_KEY;
  }

  SSL_CTX_use_PrivateKey(ssl_ctx_, pkey);
  EVP_PKEY_free(pkey);

  return CASS_OK;
}

SslContext* OpenSslContextFactory::create() {
  return new OpenSslContext();
}

void OpenSslContextFactory::init() {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  // We have to set the lock/id callbacks for use of OpenSSL thread safety.
  // It's not clear what's thread-safe in OpenSSL. Writing/Reading to
  // a single "SSL" object is NOT and we don't do that, but we do create multiple
  // "SSL" objects from a single "SSL_CTX" in different threads. That seems to be
  // okay with the following callbacks set.
  int num_locks = CRYPTO_num_locks();
  crypto_locks = new uv_rwlock_t[num_locks];
  for (int i = 0; i < num_locks; ++i) {
    if (uv_rwlock_init(crypto_locks + i)) {
      fprintf(stderr, "Unable to init read/write lock");
      abort();
    }
  }

  CRYPTO_set_locking_callback(crypto_locking_callback);
  CRYPTO_set_id_callback(crypto_id_callback);
}

} // namespace cass
