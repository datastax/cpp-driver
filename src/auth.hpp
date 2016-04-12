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
                                      DseGssapiAuthenticatorUnockCallback unlock_callback,
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
  static DseGssapiAuthenticatorUnockCallback unlock_callback_;
  static void* data_;

private:
  std::string service_;
  std::string principal_;
};

} // namespace dse

#endif
