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

#pragma once

#include <iomanip>
#include <limits>
#include <string>

#include <boost/algorithm/string/replace.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/atomic.hpp>
#include <boost/chrono.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/mutex.hpp>

#include <openssl/bn.h>
#include <openssl/crypto.h>

#include <uv.h>

#include "bridge.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "pretty_print.hpp"

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

struct CassBytes {
  CassBytes()
      : data(NULL)
      , size(0) {}
  CassBytes(const cass_byte_t* data, size_t size)
      : data(data)
      , size(size) {}
  const cass_byte_t* data;
  size_t size;
};

struct CassString {
  CassString()
      : data(NULL)
      , length(0) {}
  CassString(const char* data)
      : data(data)
      , length(strlen(data)) {}

  CassString(const char* data, size_t length)
      : data(data)
      , length(length) {}
  const char* data;
  size_t length;
};

struct CassDecimal {
  CassDecimal()
      : varint(NULL)
      , varint_size(0)
      , scale(0) {}
  CassDecimal(const cass_byte_t* varint, size_t varint_size, cass_int32_t scale)
      : varint(varint)
      , varint_size(varint_size)
      , scale(scale) {}
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
};

struct CassDuration {
  CassDuration()
      : months(0)
      , days(0)
      , nanos(0) {}
  CassDuration(cass_int32_t months, cass_int32_t days, cass_int64_t nanos)
      : months(months)
      , days(days)
      , nanos(nanos) {}
  cass_int32_t months;
  cass_int32_t days;
  cass_int64_t nanos;
};

struct CassDate {
  CassDate(cass_uint32_t date = 0)
      : date(date) {}
  operator cass_uint32_t() const { return date; }
  cass_uint32_t date;
};

struct CassTime {
  CassTime(cass_int64_t time = 0)
      : time(time) {}
  operator cass_int64_t() const { return time; }
  cass_int64_t time;
};

inline bool operator<(const CassUuid& u1, const CassUuid& u2) {
  return u1.clock_seq_and_node < u2.clock_seq_and_node || u1.time_and_version < u2.time_and_version;
}

inline bool operator==(const CassUuid& u1, const CassUuid& u2) {
  return u1.clock_seq_and_node == u2.clock_seq_and_node &&
         u1.time_and_version == u2.time_and_version;
}

inline bool operator!=(const CassUuid& u1, const CassUuid& u2) {
  return u1.clock_seq_and_node != u2.clock_seq_and_node ||
         u1.time_and_version != u2.time_and_version;
}

namespace cql {

class cql_ccm_bridge_t;

} // namespace cql

/** Random, reusable tools for testing. */
namespace test_utils {

extern const cass_duration_t ONE_SECOND_IN_MILLISECONDS;
extern const cass_duration_t ONE_MILLISECOND_IN_MICROS;
extern const cass_duration_t ONE_SECOND_IN_MICROS;

class CassLog {
public:
  CassLog() {
    // Set the maximum log level we'll just ignore anything
    // that's not relevant.
    cass_log_set_level(CASS_LOG_TRACE);
    cass_log_set_callback(CassLog::callback, &CassLog::log_data_);
  }

  static void reset(const std::string& msg) {
    log_data_.reset(msg);
    log_data_.expected_log_level_ = CASS_LOG_DISABLED;
  }

  static void add(const std::string& msg) { log_data_.add(msg); }

  static size_t message_count();

  static void set_output_log_level(CassLogLevel log_level) {
    log_data_.output_log_level = log_level;
  }

  static void set_expected_log_level(CassLogLevel log_level) {
    log_data_.expected_log_level_ = log_level;
  }

private:
  struct LogData : public boost::basic_lockable_adapter<boost::mutex> {
    LogData()
        : message_count(0)
        , expected_log_level_(CASS_LOG_DISABLED)
        , output_log_level(CASS_LOG_DISABLED) {}

    void reset(const std::string& msg) {
      boost::lock_guard<LogData> l(*this);
      messages.clear();
      add(msg);
      message_count = 0;
    }

    void add(const std::string& msg) { messages.push_back(msg); }

    boost::mutex m;
    std::vector<std::string> messages;
    size_t message_count;
    CassLogLevel expected_log_level_;
    CassLogLevel output_log_level;
  };

  static void callback(const CassLogMessage* message, void* data);

  static LogData log_data_;
};

/**
 * Simplified "Big Number" implementation for converting binary values
 */
class BigNumber {
private:
  /**
   * Decode a two's complement varint (e.g. Java BigInteger) byte array into its
   * numerical value
   *
   * @param byte_array Two's complement varint byte array
   * @return Numerical value of the varint
   */
  static std::string decode_varint(const char* byte_array) {
    std::string result;
    // Assuming positive numbers only
    // TODO: Add check for bit to return negative values
    BIGNUM* value = BN_bin2bn(reinterpret_cast<unsigned const char*>(byte_array),
                              static_cast<int>(strlen(byte_array)), NULL);
    if (value) {
      char* decimal = BN_bn2dec(value);
      result = std::string(decimal);
      OPENSSL_free(decimal);

      // Normalize - strip leading zeros
      for (unsigned int n = 0; n < result.size(); ++n) {
        if (result.at(n) == '0') {
          result.replace(n, 1, "");
        } else {
          break;
        }
      }
      if (result.size() == 0) {
        result = "0";
      }
    }

    BN_free(value);
    return result;
  }

public:
  /**
   * Convert a two's compliment byte array into a numerical string value
   *
   * @param byte_array Byte array to convert
   * @return String representation of numerical value
   */
  static std::string to_string(const char* byte_array) { return decode_varint(byte_array); }
  /**
   * Convert a <code>CassBytes<code/> object into a numerical string value
   *
   * @param byte_array Byte array to convert
   * @return String representation of numerical value
   */
  static std::string to_string(CassBytes bytes) {
    return to_string(std::string(reinterpret_cast<char const*>(bytes.data), bytes.size).c_str());
  }
  /**
   * Convert a <code>CassDecimal<code/> object into a numerical string value
   *
   * @param byte_array Byte array to convert
   * @return String representation of numerical value
   */
  static std::string to_string(CassDecimal decimal) {
    std::string byte_array =
        std::string(reinterpret_cast<const char*>(decimal.varint), decimal.varint_size);
    std::string integer_value = decode_varint(byte_array.c_str());

    // Ensure the decimal has scale
    if (decimal.scale > 0) {
      unsigned int period_position = integer_value.size() - decimal.scale;
      return integer_value.substr(0, period_position) + "." + integer_value.substr(period_position);
    }
    return integer_value;
  }
};

template <class T>
struct Deleter;

template <>
struct Deleter<CassCluster> {
  void operator()(CassCluster* ptr) {
    if (ptr != NULL) {
      cass_cluster_free(ptr);
    }
  }
};

template <>
struct Deleter<CassSession> {
  void operator()(CassSession* ptr) {
    if (ptr != NULL) {
      cass_session_free(ptr);
    }
  }
};

template <>
struct Deleter<CassFuture> {
  void operator()(CassFuture* ptr) {
    if (ptr != NULL) {
      cass_future_free(ptr);
    }
  }
};

template <>
struct Deleter<CassStatement> {
  void operator()(CassStatement* ptr) {
    if (ptr != NULL) {
      cass_statement_free(ptr);
    }
  }
};

template <>
struct Deleter<const CassResult> {
  void operator()(const CassResult* ptr) {
    if (ptr != NULL) {
      cass_result_free(ptr);
    }
  }
};

template <>
struct Deleter<const CassErrorResult> {
  void operator()(const CassErrorResult* ptr) {
    if (ptr != NULL) {
      cass_error_result_free(ptr);
    }
  }
};

template <>
struct Deleter<CassIterator> {
  void operator()(CassIterator* ptr) {
    if (ptr != NULL) {
      cass_iterator_free(ptr);
    }
  }
};

template <>
struct Deleter<CassCollection> {
  void operator()(CassCollection* ptr) {
    if (ptr != NULL) {
      cass_collection_free(ptr);
    }
  }
};

template <>
struct Deleter<CassDataType> {
  void operator()(CassDataType* ptr) {
    if (ptr != NULL) {
      cass_data_type_free(ptr);
    }
  }
};

template <>
struct Deleter<CassTuple> {
  void operator()(CassTuple* ptr) {
    if (ptr != NULL) {
      cass_tuple_free(ptr);
    }
  }
};

template <>
struct Deleter<CassUserType> {
  void operator()(CassUserType* ptr) {
    if (ptr != NULL) {
      cass_user_type_free(ptr);
    }
  }
};

template <>
struct Deleter<const CassPrepared> {
  void operator()(const CassPrepared* ptr) {
    if (ptr != NULL) {
      cass_prepared_free(ptr);
    }
  }
};

template <>
struct Deleter<CassBatch> {
  void operator()(CassBatch* ptr) {
    if (ptr != NULL) {
      cass_batch_free(ptr);
    }
  }
};

template <>
struct Deleter<CassUuidGen> {
  void operator()(CassUuidGen* ptr) {
    if (ptr != NULL) {
      cass_uuid_gen_free(ptr);
    }
  }
};

template <>
struct Deleter<const CassSchemaMeta> {
  void operator()(const CassSchemaMeta* ptr) {
    if (ptr != NULL) {
      cass_schema_meta_free(ptr);
    }
  }
};

template <>
struct Deleter<CassCustomPayload> {
  void operator()(CassCustomPayload* ptr) {
    if (ptr != NULL) {
      cass_custom_payload_free(ptr);
    }
  }
};

template <>
struct Deleter<CassRetryPolicy> {
  void operator()(CassRetryPolicy* ptr) {
    if (ptr != NULL) {
      cass_retry_policy_free(ptr);
    }
  }
};

template <class T>
class CassSharedPtr : public boost::shared_ptr<T> {
public:
  explicit CassSharedPtr(T* ptr = NULL)
      : boost::shared_ptr<T>(ptr, Deleter<T>()) {}
};

typedef CassSharedPtr<CassCluster> CassClusterPtr;
typedef CassSharedPtr<CassSession> CassSessionPtr;
typedef CassSharedPtr<CassFuture> CassFuturePtr;
typedef CassSharedPtr<CassStatement> CassStatementPtr;
typedef CassSharedPtr<const CassResult> CassResultPtr;
typedef CassSharedPtr<const CassErrorResult> CassErrorResultPtr;
typedef CassSharedPtr<CassIterator> CassIteratorPtr;
typedef CassSharedPtr<CassCollection> CassCollectionPtr;
typedef CassSharedPtr<CassDataType> CassDataTypePtr;
typedef CassSharedPtr<CassTuple> CassTuplePtr;
typedef CassSharedPtr<CassUserType> CassUserTypePtr;
typedef CassSharedPtr<const CassPrepared> CassPreparedPtr;
typedef CassSharedPtr<CassBatch> CassBatchPtr;
typedef CassSharedPtr<CassUuidGen> CassUuidGenPtr;
typedef CassSharedPtr<const CassSchemaMeta> CassSchemaMetaPtr;
typedef CassSharedPtr<CassCustomPayload> CassCustomPayloadPtr;
typedef CassSharedPtr<CassRetryPolicy> CassRetryPolicyPtr;

template <class T>
struct Value;

template <>
struct Value<cass_int8_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_int8_t value) {
    return cass_statement_bind_int8(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_int8_t value) {
    return cass_statement_bind_int8_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_int8_t value) {
    return cass_collection_append_int8(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_int8_t value) {
    return cass_tuple_set_int8(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_int8_t value) {
    return cass_user_type_set_int8(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_int8_t value) {
    return cass_user_type_set_int8_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_int8_t* output) {
    return cass_value_get_int8(value, output);
  }

  static bool equal(cass_int8_t a, cass_int8_t b) { return a == b; }

  static cass_int8_t min_value() { return std::numeric_limits<cass_int8_t>::min(); }

  static cass_int8_t max_value() { return std::numeric_limits<cass_int8_t>::max(); }

  static std::string to_string(cass_int8_t value) {
    std::stringstream value_stream;
    value_stream << static_cast<int>(value);
    return value_stream.str();
  }
};

template <>
struct Value<cass_int16_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_int16_t value) {
    return cass_statement_bind_int16(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_int16_t value) {
    return cass_statement_bind_int16_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_int16_t value) {
    return cass_collection_append_int16(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_int16_t value) {
    return cass_tuple_set_int16(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_int16_t value) {
    return cass_user_type_set_int16(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_int16_t value) {
    return cass_user_type_set_int16_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_int16_t* output) {
    return cass_value_get_int16(value, output);
  }

  static bool equal(cass_int16_t a, cass_int16_t b) { return a == b; }

  static cass_int16_t min_value() { return std::numeric_limits<cass_int16_t>::min(); }

  static cass_int16_t max_value() { return std::numeric_limits<cass_int16_t>::max(); }

  static std::string to_string(cass_int16_t value) {
    std::stringstream value_stream;
    value_stream << value;
    return value_stream.str();
  }
};

template <>
struct Value<cass_int32_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_int32_t value) {
    return cass_statement_bind_int32(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_int32_t value) {
    return cass_statement_bind_int32_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_int32_t value) {
    return cass_collection_append_int32(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_int32_t value) {
    return cass_tuple_set_int32(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_int32_t value) {
    return cass_user_type_set_int32(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_int32_t value) {
    return cass_user_type_set_int32_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_int32_t* output) {
    return cass_value_get_int32(value, output);
  }

  static bool equal(cass_int32_t a, cass_int32_t b) { return a == b; }

  static cass_int32_t min_value() { return std::numeric_limits<cass_int32_t>::min(); }

  static cass_int32_t max_value() { return std::numeric_limits<cass_int32_t>::max(); }

  static std::string to_string(cass_int32_t value) {
    std::stringstream value_stream;
    value_stream << value;
    return value_stream.str();
  }
};

template <>
struct Value<CassDate> {
  static CassError bind(CassStatement* statement, size_t index, CassDate value) {
    return cass_statement_bind_uint32(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassDate value) {
    return cass_statement_bind_uint32_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, CassDate value) {
    return cass_collection_append_uint32(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassDate value) {
    return cass_tuple_set_uint32(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassDate value) {
    return cass_user_type_set_uint32(user_type, index, value);
  }

  static CassError get(const CassValue* value, CassDate* output) {
    return cass_value_get_uint32(value, &output->date);
  }

  static bool equal(CassDate a, CassDate b) { return a == b; }

  static CassDate min_value() {
    return 2147483648u; // This is the minimum value supported by strftime()
  }

  static CassDate max_value() {
    return 2147533357u; // This is the maximum value supported by strftime()
  }

  static std::string to_string(CassDate value) {
    char temp[32];
    time_t epoch_secs = static_cast<time_t>(cass_date_time_to_epoch(value, 0));
    strftime(temp, sizeof(temp), "'%Y-%m-%d'", gmtime(&epoch_secs));
    return temp;
  }
};

template <>
struct Value<CassTime> {
  static CassError bind(CassStatement* statement, size_t index, CassTime value) {
    return cass_statement_bind_int64(statement, index, value.time);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassTime value) {
    return cass_statement_bind_int64_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, CassTime value) {
    return cass_collection_append_int64(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassTime value) {
    return cass_tuple_set_int64(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassTime value) {
    return cass_user_type_set_int64(user_type, index, value);
  }

  static CassError get(const CassValue* value, CassTime* output) {
    return cass_value_get_int64(value, &output->time);
  }

  static bool equal(CassTime a, CassTime b) { return a == b; }

  static CassTime min_value() { return 0; }

  static CassTime max_value() { return 86399999999999; }

  static std::string to_string(CassTime value) {
    char temp[32];
    time_t epoch_secs = static_cast<time_t>(cass_date_time_to_epoch(2147483648, value));
    struct tm* time = gmtime(&epoch_secs);
    strftime(temp, sizeof(temp), "'%H:%M:%S", time);
    std::string str(temp);
    cass_int64_t diff = value - epoch_secs * 1000000000;
    sprintf(temp, "%09u", (unsigned int)diff);
    str.append(".");
    str.append(temp);
    str.append("'");
    return str;
  }
};

template <>
struct Value<cass_int64_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_int64_t value) {
    return cass_statement_bind_int64(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_int64_t value) {
    return cass_statement_bind_int64_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_int64_t value) {
    return cass_collection_append_int64(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_int64_t value) {
    return cass_tuple_set_int64(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_int64_t value) {
    return cass_user_type_set_int64(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_int64_t value) {
    return cass_user_type_set_int64_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_int64_t* output) {
    return cass_value_get_int64(value, output);
  }

  static bool equal(cass_int64_t a, cass_int64_t b) { return a == b; }

  static cass_int64_t min_value() { return std::numeric_limits<cass_int64_t>::min(); }

  static cass_int64_t max_value() { return std::numeric_limits<cass_int64_t>::max(); }

  static std::string to_string(cass_int64_t value) {
    std::stringstream value_stream;
    value_stream << value;
    return value_stream.str();
  }
};

template <>
struct Value<cass_float_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_float_t value) {
    return cass_statement_bind_float(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_float_t value) {
    return cass_statement_bind_float_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_float_t value) {
    return cass_collection_append_float(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_float_t value) {
    return cass_tuple_set_float(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_float_t value) {
    return cass_user_type_set_float(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_float_t value) {
    return cass_user_type_set_float_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_float_t* output) {
    return cass_value_get_float(value, output);
  }

  static bool equal(cass_float_t a, cass_float_t b) { return a == b; }

  static cass_float_t min_value() { return std::numeric_limits<cass_float_t>::min(); }

  static cass_float_t max_value() { return std::numeric_limits<cass_float_t>::max(); }

  static std::string to_string(cass_float_t value) {
    std::stringstream value_stream;
    value_stream << std::setprecision(32) << value;
    return value_stream.str();
  }
};

template <>
struct Value<cass_double_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_double_t value) {
    return cass_statement_bind_double(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_double_t value) {
    return cass_statement_bind_double_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_double_t value) {
    return cass_collection_append_double(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_double_t value) {
    return cass_tuple_set_double(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_double_t value) {
    return cass_user_type_set_double(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_double_t value) {
    return cass_user_type_set_double_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_double_t* output) {
    return cass_value_get_double(value, output);
  }

  static bool equal(cass_double_t a, cass_double_t b) { return a == b; }

  static cass_double_t min_value() { return std::numeric_limits<cass_double_t>::min(); }

  static cass_double_t max_value() { return std::numeric_limits<cass_double_t>::max(); }

  static std::string to_string(cass_double_t value) {
    std::stringstream value_stream;
    value_stream << std::setprecision(32) << value;
    return value_stream.str();
  }
};

template <>
struct Value<cass_bool_t> {
  static CassError bind(CassStatement* statement, size_t index, cass_bool_t value) {
    return cass_statement_bind_bool(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, cass_bool_t value) {
    return cass_statement_bind_bool_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, cass_bool_t value) {
    return cass_collection_append_bool(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, cass_bool_t value) {
    return cass_tuple_set_bool(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, cass_bool_t value) {
    return cass_user_type_set_bool(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         cass_bool_t value) {
    return cass_user_type_set_bool_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, cass_bool_t* output) {
    return cass_value_get_bool(value, output);
  }

  static bool equal(cass_bool_t a, cass_bool_t b) { return a == b; }

  static std::string to_string(cass_bool_t value) { return value == cass_true ? "TRUE" : "FALSE"; }
};

template <>
struct Value<CassString> {
  static CassError bind(CassStatement* statement, size_t index, CassString value) {
    return cass_statement_bind_string_n(statement, index, value.data, value.length);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassString value) {
    return cass_statement_bind_string_by_name_n(statement, name, strlen(name), value.data,
                                                value.length);
  }

  static CassError append(CassCollection* collection, CassString value) {
    return cass_collection_append_string_n(collection, value.data, value.length);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassString value) {
    return cass_tuple_set_string_n(tuple, index, value.data, value.length);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassString value) {
    return cass_user_type_set_string_n(user_type, index, value.data, value.length);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassString value) {
    return cass_user_type_set_string_by_name_n(user_type, name, strlen(name), value.data,
                                               value.length);
  }

  static CassError get(const CassValue* value, CassString* output) {
    return cass_value_get_string(value, &output->data, &output->length);
  }

  static bool equal(CassString a, CassString b) {
    if (a.length != b.length) {
      return false;
    }
    return strncmp(a.data, b.data, a.length) == 0;
  }

  static std::string to_string(CassString value) { return std::string(value.data, value.length); }
};

template <>
struct Value<CassBytes> {
  static CassError bind(CassStatement* statement, size_t index, CassBytes value) {
    return cass_statement_bind_bytes(statement, index, value.data, value.size);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassBytes value) {
    return cass_statement_bind_bytes_by_name(statement, name, value.data, value.size);
  }

  static CassError append(CassCollection* collection, CassBytes value) {
    return cass_collection_append_bytes(collection, value.data, value.size);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassBytes value) {
    return cass_tuple_set_bytes(tuple, index, value.data, value.size);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassBytes value) {
    return cass_user_type_set_bytes(user_type, index, value.data, value.size);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassBytes value) {
    return cass_user_type_set_bytes_by_name(user_type, name, value.data, value.size);
  }

  static CassError get(const CassValue* value, CassBytes* output) {
    return cass_value_get_bytes(value, &output->data, &output->size);
  }

  static bool equal(CassBytes a, CassBytes b) {
    if (a.size != b.size) {
      return false;
    }
    return memcmp(a.data, b.data, a.size) == 0;
  }

  static std::string to_string(CassBytes value) {
    return std::string(reinterpret_cast<char const*>(value.data), value.size);
  }
};

template <>
struct Value<CassInet> {
  static CassError bind(CassStatement* statement, size_t index, CassInet value) {
    return cass_statement_bind_inet(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassInet value) {
    return cass_statement_bind_inet_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, CassInet value) {
    return cass_collection_append_inet(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassInet value) {
    return cass_tuple_set_inet(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassInet value) {
    return cass_user_type_set_inet(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassInet value) {
    return cass_user_type_set_inet_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, CassInet* output) {
    return cass_value_get_inet(value, output);
  }

  static bool equal(CassInet a, CassInet b) {
    if (a.address_length != b.address_length) {
      return false;
    }
    return memcmp(a.address, b.address, a.address_length) == 0;
  }

  static CassInet min_value() {
    CassInet value;
    value.address_length = 16;
    memset(value.address, 0x0, sizeof(value.address));
    return value;
  }

  static CassInet max_value() {
    CassInet value;
    value.address_length = 16;
    memset(value.address, 0xF, sizeof(value.address));
    return value;
  }

  static std::string to_string(CassInet value) {
    char buffer[INET6_ADDRSTRLEN];
    if (value.address_length == 4) {
      uv_inet_ntop(AF_INET, value.address, buffer, sizeof(buffer));
    } else {
      uv_inet_ntop(AF_INET6, value.address, buffer, sizeof(buffer));
    }

    return std::string(buffer);
  }
};

template <>
struct Value<CassUuid> {
  static CassError bind(CassStatement* statement, size_t index, CassUuid value) {
    return cass_statement_bind_uuid(statement, index, value);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassUuid value) {
    return cass_statement_bind_uuid_by_name(statement, name, value);
  }

  static CassError append(CassCollection* collection, CassUuid value) {
    return cass_collection_append_uuid(collection, value);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassUuid value) {
    return cass_tuple_set_uuid(tuple, index, value);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassUuid value) {
    return cass_user_type_set_uuid(user_type, index, value);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassUuid value) {
    return cass_user_type_set_uuid_by_name(user_type, name, value);
  }

  static CassError get(const CassValue* value, CassUuid* output) {
    return cass_value_get_uuid(value, output);
  }

  static bool equal(CassUuid a, CassUuid b) {
    return a.clock_seq_and_node == b.clock_seq_and_node && a.time_and_version == b.time_and_version;
  }

  static CassUuid min_value() {
    CassUuid value;
    value.clock_seq_and_node = 0;
    value.time_and_version = 0;
    return value;
  }

  static CassUuid max_value() {
    CassUuid value;
    value.clock_seq_and_node = std::numeric_limits<cass_uint64_t>::max();
    value.time_and_version = std::numeric_limits<cass_uint64_t>::max();
    return value;
  }

  static std::string to_string(CassUuid value) {
    char c_string[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(value, c_string);
    return std::string(c_string);
  }
};

template <>
struct Value<CassDecimal> {
  static CassError bind(CassStatement* statement, size_t index, CassDecimal value) {
    return cass_statement_bind_decimal(statement, index, value.varint, value.varint_size,
                                       value.scale);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassDecimal value) {
    return cass_statement_bind_decimal_by_name(statement, name, value.varint, value.varint_size,
                                               value.scale);
  }

  static CassError append(CassCollection* collection, CassDecimal value) {
    return cass_collection_append_decimal(collection, value.varint, value.varint_size, value.scale);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassDecimal value) {
    return cass_tuple_set_decimal(tuple, index, value.varint, value.varint_size, value.scale);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassDecimal value) {
    return cass_user_type_set_decimal(user_type, index, value.varint, value.varint_size,
                                      value.scale);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassDecimal value) {
    return cass_user_type_set_decimal_by_name(user_type, name, value.varint, value.varint_size,
                                              value.scale);
  }

  static CassError get(const CassValue* value, CassDecimal* output) {
    return cass_value_get_decimal(value, &output->varint, &output->varint_size, &output->scale);
  }

  static bool equal(CassDecimal a, CassDecimal b) {
    if (a.scale != b.scale) {
      return false;
    }
    if (a.varint_size != b.varint_size) {
      return false;
    }
    return memcmp(a.varint, b.varint, a.varint_size) == 0;
  }

  static std::string to_string(CassDecimal value) {
    return test_utils::BigNumber::to_string(value);
  }
};

template <>
struct Value<CassDuration> {
  static CassError bind(CassStatement* statement, size_t index, CassDuration value) {
    return cass_statement_bind_duration(statement, index, value.months, value.days, value.nanos);
  }

  static CassError bind_by_name(CassStatement* statement, const char* name, CassDuration value) {
    return cass_statement_bind_duration_by_name(statement, name, value.months, value.days,
                                                value.nanos);
  }

  static CassError append(CassCollection* collection, CassDuration value) {
    return cass_collection_append_duration(collection, value.months, value.days, value.nanos);
  }

  static CassError tuple_set(CassTuple* tuple, size_t index, CassDuration value) {
    return cass_tuple_set_duration(tuple, index, value.months, value.days, value.nanos);
  }

  static CassError user_type_set(CassUserType* user_type, size_t index, CassDuration value) {
    return cass_user_type_set_duration(user_type, index, value.months, value.days, value.nanos);
  }

  static CassError user_type_set_by_name(CassUserType* user_type, const char* name,
                                         CassDuration value) {
    return cass_user_type_set_duration_by_name(user_type, name, value.months, value.days,
                                               value.nanos);
  }

  static CassError get(const CassValue* value, CassDuration* output) {
    return cass_value_get_duration(value, &output->months, &output->days, &output->nanos);
  }

  static bool equal(CassDuration a, CassDuration b) {
    return a.months == b.months && a.days == b.days && a.nanos == b.nanos;
  }

  static std::string to_string(CassDuration value) {
    // String representation of Duration is wonky in C*. (-3, -2, -1) is represented by
    // -3mo2d1ns. There is no way to represent a mix of positive and negative attributes. We
    // tippy-toe around this in our testing...
    std::ostringstream buf;
    buf << value.months << "mo" << (value.days < 0 ? -value.days : value.days) << "d"
        << (value.nanos < 0 ? -value.nanos : value.nanos) << "ns";
    return buf.str();
  }
};

/*
 * TODO: Implement https://datastax-oss.atlassian.net/browse/CPP-244 to avoid
 *       current test skip implementation in batch and serial_consistency.
 */
/** The following class cannot be used as a kernel of test fixture because of
    parameterized ctor. Derive from it to use it in your tests.
 */
struct MultipleNodesTest {
  MultipleNodesTest(unsigned int num_nodes_dc1, unsigned int num_nodes_dc2,
                    unsigned int protocol_version = CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION,
                    bool with_vnodes = false, bool is_ssl = false);

  bool check_version(const std::string& required);
  bool is_beta_protocol();

  virtual ~MultipleNodesTest();

  boost::shared_ptr<CCM::Bridge> ccm;
  static CCM::CassVersion version;
  CassUuidGen* uuid_gen;
  CassCluster* cluster;
};

struct SingleSessionTest : public MultipleNodesTest {
  SingleSessionTest(unsigned int num_nodes_dc1, unsigned int num_nodes_dc2,
                    bool with_session = true,
                    unsigned int protocol_version = CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION,
                    bool with_vnodes = false, bool is_ssl = false);

  virtual ~SingleSessionTest();
  void create_session();
  void close_session();

  CassSession* session;
  CassSsl* ssl;
};

void initialize_contact_points(CassCluster* cluster, std::string prefix, unsigned int num_of_nodes);

const char* get_value_type(CassValueType type);

/**
 * Convert a byte array to hex
 *
 * @param byte_array Byte array to convert
 * @return Hex representation of the byte array
 */
std::string to_hex(const char* byte_array);

/**
 * Find and replace all occurrences of a string and replace with a new value
 *
 * @param current The string to replace all occurrences
 * @param search The string to find
 * @param replace The string to replace with
 * @return Updated string
 */
std::string replaceAll(const std::string& current, const std::string& search,
                       const std::string& replace);

CassSessionPtr create_session(CassCluster* cluster,
                              cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);
CassSessionPtr create_session(CassCluster* cluster, CassError* code,
                              cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);

CassError execute_query_with_error(CassSession* session, const std::string& query,
                                   CassResultPtr* result = NULL,
                                   CassConsistency consistency = CASS_CONSISTENCY_ONE,
                                   cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);

void execute_query(CassSession* session, const std::string& query, CassResultPtr* result = NULL,
                   CassConsistency consistency = CASS_CONSISTENCY_ONE,
                   cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);

CassError wait_and_return_error(CassFuture* future,
                                cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);
void wait_and_check_error(CassFuture* future, cass_duration_t timeout = 60 * ONE_SECOND_IN_MICROS);

test_utils::CassPreparedPtr prepare(CassSession* session, const std::string& query);

inline CassBytes bytes_from_string(const char* str) {
  return CassBytes(reinterpret_cast<const cass_uint8_t*>(str), strlen(str));
}

inline CassInet inet_v4_from_int(int32_t address) {
  CassInet inet;
  memcpy(inet.address, &address, sizeof(int32_t));
  inet.address_length = sizeof(int32_t);
  return inet;
}

inline CassUuid generate_time_uuid(CassUuidGen* uuid_gen) {
  CassUuid uuid;
  cass_uuid_gen_time(uuid_gen, &uuid);
  return uuid;
}

inline CassUuid generate_random_uuid(CassUuidGen* uuid_gen) {
  CassUuid uuid;
  cass_uuid_gen_random(uuid_gen, &uuid);
  return uuid;
}

inline std::string generate_unique_str(CassUuidGen* uuid_gen) {
  CassUuid uuid;
  cass_uuid_gen_time(uuid_gen, &uuid);
  char buffer[CASS_UUID_STRING_LENGTH];
  cass_uuid_string(uuid, buffer);
  return boost::replace_all_copy(std::string(buffer), "-", "");
}

std::string string_from_time_point(boost::chrono::system_clock::time_point time);
std::string string_from_uuid(CassUuid uuid);

inline std::string generate_random_uuid_str(CassUuidGen* uuid_gen) {
  return string_from_uuid(generate_random_uuid(uuid_gen));
}

/**
 * Get the Cassandra version number from current session or from the
 * configuration file.
 *
 * @param session Current connected session (default: NULL; get version from
 *                configuration file)
 * @return Cassandra version from session or configuration file
 */
CCM::CassVersion get_version(CassSession* session = NULL);

/*
 * Generate a random string of a certain size using alpha numeric characters
 *
 * @param size Size of the string (in bytes) [default: 1024]
 */
std::string generate_random_string(unsigned int size = 1024);

/**
 * Load the PEM SSL certificate
 *
 * @return String representing the PEM certificate
 */
std::string load_ssl_certificate(const std::string filename);

/**
 * Concatenate an array/vector into a string
 *
 * @param elements Array/Vector elements to concatenate
 * @param delimiter Character to use between elements (default: <space>)
 * @param delimiter_prefix Character to use before delimiter (default: <empty>)
 * @param delimiter_suffix Character to use after delimiter (default: <empty>)
 * @return A string representation of all the array/vector elements
 */
std::string implode(const std::vector<std::string>& elements, const char delimiter = ' ',
                    const char* delimiter_prefix = NULL, const char* delimiter_suffix = NULL);

/**
 * Wait for the driver to establish connection to a given node
 *
 * @param ip_prefix IPv4 prefix for node
 * @param node Node to wait for
 * @param total_attempts Total number of attempts to wait on connection
 */
void wait_for_node_connection(const std::string& ip_prefix, int node, int total_attempts = 10);

/**
 * Wait for the driver to establish connection to a given set of nodes
 *
 * @param ip_prefix IPv4 prefix for node(s)
 * @param nodes List of nodes to wait for
 * @param total_attempts Total number of attempts to wait on connection
 *                       (default: 10)
 */
void wait_for_node_connections(const std::string& ip_prefix, std::vector<int> nodes,
                               int total_attempts = 10);

/**
 * Trim whitespace from the front and back of a string
 *
 * @param str The string to trim
 * @return A reference to the modified string with its whitespace trimmed
 */
std::string& trim(std::string& str);

/**
 * Split a string into pieces using a provided delimiter charactor
 *
 * @param str The string to explode
 * @param vec The result
 * @param delimiter The character used to divide the string
 */
void explode(const std::string& str, std::vector<std::string>& vec, const char delimiter = ',');

extern const char* CREATE_TABLE_ALL_TYPES;
extern const char* CREATE_TABLE_ALL_TYPES_V4;
extern const char* CREATE_TABLE_ALL_TYPES_V4_1;
extern const char* CREATE_TABLE_TIME_SERIES;
extern const char* CREATE_TABLE_SIMPLE;

extern const std::string CREATE_KEYSPACE_SIMPLE_FORMAT;
extern const std::string CREATE_KEYSPACE_NETWORK_FORMAT;
extern const std::string CREATE_KEYSPACE_GENERIC_FORMAT;
extern const std::string DROP_KEYSPACE_FORMAT;
extern const std::string DROP_KEYSPACE_IF_EXISTS_FORMAT;
extern const std::string SIMPLE_KEYSPACE;
extern const std::string SIMPLE_TABLE;
extern const std::string CREATE_TABLE_SIMPLE_FORMAT;
extern const std::string INSERT_FORMAT;
extern const std::string SELECT_ALL_FORMAT;
extern const std::string SELECT_WHERE_FORMAT;
extern const std::string SELECT_VERSION;
extern const std::string lorem_ipsum;
extern const char ALPHA_NUMERIC[];

} // End of namespace test_utils

inline bool operator==(CassString a, CassString b) {
  return test_utils::Value<CassString>::equal(a, b);
}

inline bool operator<(CassString a, CassString b) {
  if (a.length > b.length) {
    return false;
  } else if (a.length < b.length) {
    return true;
  }
  return strncmp(a.data, b.data, a.length) < 0;
}

inline bool operator==(CassBytes a, CassBytes b) {
  return test_utils::Value<CassBytes>::equal(a, b);
}

inline bool operator<(CassBytes a, CassBytes b) {
  if (a.size > b.size) {
    return false;
  } else if (a.size < b.size) {
    return true;
  }
  return memcmp(a.data, b.data, a.size) < 0;
}

inline bool operator==(CassInet a, CassInet b) { return test_utils::Value<CassInet>::equal(a, b); }

inline bool operator<(CassInet a, CassInet b) {
  if (a.address_length > b.address_length) {
    return false;
  } else if (a.address_length < b.address_length) {
    return true;
  }
  return memcmp(a.address, b.address, a.address_length) < 0;
}

inline bool operator==(CassDecimal a, CassDecimal b) {
  return test_utils::Value<CassDecimal>::equal(a, b);
}

inline bool operator<(CassDecimal a, CassDecimal b) {
  // TODO: This might not be exactly correct, but should work
  // for testing
  if (a.scale > b.scale) {
    return false;
  } else if (a.scale < b.scale) {
    return true;
  }
  if (a.varint_size > b.varint_size) {
    return false;
  } else if (a.varint_size < b.varint_size) {
    return true;
  }
  return memcmp(a.varint, b.varint, a.varint_size) < 0;
}

inline bool operator==(CassDuration a, CassDuration b) {
  return a.months == b.months && a.days == b.days && a.nanos == b.nanos;
}

inline bool operator<(CassDuration a, CassDuration b) {
  if (a.months > b.months) {
    return false;
  } else if (a.months < b.months) {
    return true;
  }

  if (a.days > b.days) {
    return false;
  } else if (a.days < b.days) {
    return true;
  }

  return a.nanos < b.nanos;
}
