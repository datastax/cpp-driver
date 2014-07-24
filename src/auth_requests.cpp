#include "auth_requests.hpp"

namespace cass {

int CredentialsRequest::encode(int version, BufferVec* bufs) const {
  if (version != 1) {
    return -1;
  }
  return 0;
}

int AuthResponseRequest::encode(int version, BufferVec* bufs) const {
  if (version < 2) {
    return -1;
  }
  return 0;
}

} // namespace cass
