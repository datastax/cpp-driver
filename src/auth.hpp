/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_AUTH_HPP_INCLUDED__
#define __DSE_AUTH_HPP_INCLUDED__

#include "dse.h"

#include <string>
#include <uv.h>

namespace dse {

class PlaintextAuthenticatorData {
public:
  PlaintextAuthenticatorData(const std::string& username,
                             const std::string& password)
    : username_(username)
    , password_(password) { }

  static const CassAuthenticatorCallbacks* callbacks() { return &callbacks_; }

private:
  static void on_initial(CassAuthenticator* auth, void* data);

  static void on_challenge(CassAuthenticator* auth, void* data,
                           const char* token, size_t token_size);
private:
  static CassAuthenticatorCallbacks callbacks_;

private:
  std::string username_;
  std::string password_;
};

class GssapiAuthenticatorData {
public:
  GssapiAuthenticatorData(const std::string& service,
                          const std::string& principal)
    : service_(service)
    , principal_(principal) { }

  static const CassAuthenticatorCallbacks* callbacks() { return &callbacks_; }

  const std::string& service() const { return service_; }
  const std::string& principal() const { return principal_; }

  static CassError set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                      DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                      void* data);

  static void lock() { lock_callback_(data_); }
  static void unlock() { unlock_callback_(data_); }

private:
  static void on_initial(CassAuthenticator* auth, void* data);

  static void on_challenge(CassAuthenticator* auth, void* data,
                           const char* token, size_t token_size);

  static void on_cleanup(CassAuthenticator* auth, void* data);


private:
  static CassAuthenticatorCallbacks callbacks_;
  static DseGssapiAuthenticatorLockCallback lock_callback_;
  static DseGssapiAuthenticatorUnlockCallback unlock_callback_;
  static void* data_;

private:
  std::string service_;
  std::string principal_;
};

} // namespace dse

#endif
