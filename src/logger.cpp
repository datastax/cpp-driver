#include "logger.hpp"

namespace cass {

void default_log_callback(void* data, cass_uint64_t time, CassLogLevel severity,
                          CassString message) {
  // TODO(mpenick): Format time
  fprintf(stderr, "[%s]: %.*s\n", cass_log_level_string(severity),
          static_cast<int>(message.length), message.data);
}
}
