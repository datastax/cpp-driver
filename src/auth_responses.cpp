#include "auth_responses.hpp"
#include "serialization.hpp"

namespace cass {

bool AuthenticateResponse::decode(int version, char* buffer, size_t size) {
  if (version != 1) {
    return false;
  }
  decode_string(buffer, &authenticator_, authenticator_size_);
  return true;
}

bool cass::AuthChallengeResponse::decode(int version, char* buffer, size_t size) {
  if (version < 2) {
    return false;
  }
  decode_bytes(buffer, &token_, token_size_);
  return true;
}

bool cass::AuthSuccessResponse::decode(int version, char* buffer, size_t size) {
  if (version < 2) {
    return false;
  }
  decode_bytes(buffer, &token_, token_size_);
  return true;
}

} // namespace cass
