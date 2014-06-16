#include "config.hpp"
#include "common.hpp"

#include <sstream>

namespace cass {

CassError Config::set_option(CassOption option, const void* value,
                             size_t size) {
#define CHECK_SIZE_AND_COPY(dst, dst_size)       \
  do {                                           \
    if (size < dst_size) {                       \
      return CASS_ERROR_LIB_INVALID_OPTION_SIZE; \
    }                                            \
    memcpy(dst, value, dst_size);                \
  } while (0)

  switch (option) {
    case CASS_OPTION_PORT:
      CHECK_SIZE_AND_COPY(&port_, sizeof(port_));
      break;
    case CASS_OPTION_CQL_VERSION:
      version_ = std::string(reinterpret_cast<const char*>(value), size);
      break;
    case CASS_OPTION_NUM_THREADS_IO:
      CHECK_SIZE_AND_COPY(&thread_count_io_, sizeof(thread_count_io_));
      break;
    case CASS_OPTION_QUEUE_SIZE_IO:
      CHECK_SIZE_AND_COPY(&queue_size_io_, sizeof(queue_size_io_));
      break;
    case CASS_OPTION_QUEUE_SIZE_EVENTS:
      CHECK_SIZE_AND_COPY(&queue_size_event_, sizeof(queue_size_event_));
      break;
    case CASS_OPTION_CONTACT_POINTS:
      if (size == 0) {
        contact_points_.clear();
      } else {
        std::istringstream stream(
            std::string(reinterpret_cast<const char*>(value), size));
        while (!stream.eof()) {
          std::string contact_point;
          std::getline(stream, contact_point, ',');
          if (!trim(contact_point).empty()) {
            contact_points_.push_back(contact_point);
          }
        }
      }
      break;
    case CASS_OPTION_CORE_CONNECTIONS_PER_HOST:
      CHECK_SIZE_AND_COPY(&core_connections_per_host_,
                          sizeof(core_connections_per_host_));
      break;
    case CASS_OPTION_MAX_CONNECTIONS_PER_HOST:
      CHECK_SIZE_AND_COPY(&max_connections_per_host_,
                          sizeof(max_connections_per_host_));
      break;
    case CASS_OPTION_MAX_SIMULTANEOUS_CREATION:
      CHECK_SIZE_AND_COPY(&max_simultaneous_creation_,
                          sizeof(max_simultaneous_creation_));
      break;
    case CASS_RECONNECT_WAIT_TIME:
      CHECK_SIZE_AND_COPY(&reconnect_wait_time_, sizeof(reconnect_wait_time_));
      break;
    case CASS_OPTION_CONNECT_TIMEOUT:
      CHECK_SIZE_AND_COPY(&connect_timeout_, sizeof(connect_timeout_));
      break;
    case CASS_OPTION_WRITE_TIMEOUT:
      CHECK_SIZE_AND_COPY(&write_timeout_, sizeof(write_timeout_));
      break;
    case CASS_OPTION_READ_TIMEOUT:
      CHECK_SIZE_AND_COPY(&read_timeout_, sizeof(read_timeout_));
      break;
    case CASS_OPTION_LOG_LEVEL:
      CHECK_SIZE_AND_COPY(&log_level_, sizeof(log_level_));
      break;
    case CASS_OPTION_LOG_DATA:
      CHECK_SIZE_AND_COPY(&log_data_, sizeof(log_data_));
      break;
    case CASS_OPTION_LOG_CALLBACK:
      CHECK_SIZE_AND_COPY(&log_callback_, sizeof(log_callback_));
      break;
    default:
      return CASS_ERROR_LIB_INVALID_OPTION;
  }

#undef CHECK_SIZE_AND_COPY

  return CASS_OK;
}

CassError Config::option(CassOption option, void* value, size_t* size) const {
  if (size == nullptr) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

#define CHECK_SIZE_AND_COPY(src, src_size)       \
  do {                                           \
    if (*size < src_size) {                      \
      *size = src_size;                          \
      return CASS_ERROR_LIB_INVALID_OPTION_SIZE; \
    }                                            \
    *size = src_size;                            \
    memcpy(value, src, src_size);                \
  } while (0)

  switch (option) {
    case CASS_OPTION_PORT:
      CHECK_SIZE_AND_COPY(&port_, sizeof(port_));
      break;
    case CASS_OPTION_CQL_VERSION: {
      if (*size <= version_.size()) {
        *size = version_.size() + 1;
        return CASS_ERROR_LIB_INVALID_OPTION_SIZE;
      }
      char* output = reinterpret_cast<char*>(value);
      version_.copy(output, *size);
      output[*size] = '\0';
    } break;
    case CASS_OPTION_NUM_THREADS_IO:
      CHECK_SIZE_AND_COPY(&thread_count_io_, sizeof(thread_count_io_));
      break;
    case CASS_OPTION_QUEUE_SIZE_IO:
      CHECK_SIZE_AND_COPY(&queue_size_io_, sizeof(queue_size_io_));
      break;
    case CASS_OPTION_QUEUE_SIZE_EVENTS:
      CHECK_SIZE_AND_COPY(&queue_size_event_, sizeof(queue_size_event_));
      break;
    case CASS_OPTION_CONTACT_POINTS:
      if (contact_points_.empty()) {
        *size = 0;
      } else {
        size_t total_size = 0;
        for (auto& contact_point : contact_points_) {
          total_size += contact_point.size() + 1; // space for ',' or '\0'
        }

        if (*size < total_size) {
          *size = total_size;
          return CASS_ERROR_LIB_INVALID_OPTION_SIZE;
        }
        *size = total_size;

        size_t pos = 0;
        char* output = reinterpret_cast<char*>(value);
        for (auto& contact_point : contact_points_) {
          if (pos > 0) {
            output[pos++] = ',';
          }
          contact_point.copy(output + pos, contact_point.size());
          pos += contact_point.size();
        }
        output[pos] = '\0';
      }
      break;
    case CASS_OPTION_CORE_CONNECTIONS_PER_HOST:
      CHECK_SIZE_AND_COPY(&core_connections_per_host_,
                          sizeof(core_connections_per_host_));
      break;
    case CASS_OPTION_MAX_CONNECTIONS_PER_HOST:
      CHECK_SIZE_AND_COPY(&max_connections_per_host_,
                          sizeof(max_connections_per_host_));
      break;
    case CASS_OPTION_MAX_SIMULTANEOUS_CREATION:
      CHECK_SIZE_AND_COPY(&max_simultaneous_creation_,
                          sizeof(max_simultaneous_creation_));
      break;
    case CASS_RECONNECT_WAIT_TIME:
      CHECK_SIZE_AND_COPY(&reconnect_wait_time_, sizeof(reconnect_wait_time_));
      break;
    case CASS_OPTION_CONNECT_TIMEOUT:
      CHECK_SIZE_AND_COPY(&connect_timeout_, sizeof(connect_timeout_));
      break;
    case CASS_OPTION_WRITE_TIMEOUT:
      CHECK_SIZE_AND_COPY(&write_timeout_, sizeof(write_timeout_));
      break;
    case CASS_OPTION_READ_TIMEOUT:
      CHECK_SIZE_AND_COPY(&read_timeout_, sizeof(read_timeout_));
      break;
    case CASS_OPTION_LOG_LEVEL:
      CHECK_SIZE_AND_COPY(&log_level_, sizeof(log_level_));
      break;
    case CASS_OPTION_LOG_DATA:
      CHECK_SIZE_AND_COPY(&log_data_, sizeof(log_data_));
      break;
    case CASS_OPTION_LOG_CALLBACK:
      CHECK_SIZE_AND_COPY(&log_callback_, sizeof(log_callback_));
      break;
    default:
      return CASS_ERROR_LIB_INVALID_OPTION;
  }

#undef CHECK_SIZE_AND_COPY

  return CASS_OK;
}

} // namespace cass
