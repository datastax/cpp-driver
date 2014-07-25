#include "auth_requests.hpp"

namespace cass {

int CredentialsRequest::encode(int version, BufferVec* bufs) const {
  if (version != 1) {
    return -1;
  }

  // <n> [short] <pair_0> [string][string] ... <pair_n> [string][string]
  size_t length = sizeof(uint16_t);

  for (V1Authenticator::Credentials::const_iterator it = credentials_.begin(),
       end = credentials_.end(); it != end; ++it) {
    length += sizeof(uint16_t) + it->first.size();
    length += sizeof(uint16_t) + it->second.size();
  }

  Buffer buf(length);
  buf.encode_string_map(0, credentials_);
  bufs->push_back(buf);

  return length;
}

int AuthResponseRequest::encode(int version, BufferVec* bufs) const {
  if (version < 2) {
    return -1;
  }

  // <token> [bytes]
  size_t length = sizeof(int32_t) + token_.size();

  Buffer buf(length);
  buf.encode_long_string(0, token_.data(), token_.size());
  bufs->push_back(buf);

  return length;
}

} // namespace cass
