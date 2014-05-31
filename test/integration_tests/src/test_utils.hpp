#pragma once

#include <boost/asio/ip/address.hpp>
#include <boost/shared_ptr.hpp>

#include "cassandra.h"

#include "cql_ccm_bridge_configuration.hpp"

namespace cql {

class cql_ccm_bridge_t;

} // namespace cql

/** Random, reusable tools for testing. */
namespace test_utils {

constexpr cass_duration_t ONE_MILLISECOND_IN_MICROS = 1000; // in microseconds
constexpr cass_duration_t ONE_SECOND_IN_MICROS = 1000 * ONE_MILLISECOND_IN_MICROS;

template<class T>
struct StackPtrFree;

template<>
struct StackPtrFree<CassSession> {
    static void free(CassSession* ptr) {
      CassFuture* future = cass_session_shutdown(ptr);
      cass_future_wait(future);
      cass_session_free(ptr);
      cass_future_free(future);
    }
};

template<>
struct StackPtrFree<CassFuture> {
    static void free(CassFuture* ptr) {
      cass_future_free(ptr);
    }
};

template<>
struct StackPtrFree<CassStatement> {
    static void free(CassStatement* ptr) {
      cass_statement_free(ptr);
    }
};

template<>
struct StackPtrFree<const CassResult> {
    static void free(const CassResult* ptr) {
      cass_result_free(ptr);
    }
};

template<>
struct StackPtrFree<CassIterator> {
    static void free(CassIterator* ptr) {
      cass_iterator_free(ptr);
    }
};


template<class T>
class StackPtr {
  public:
    StackPtr() : ptr_(nullptr) { }
    StackPtr(T* ptr) : ptr_(ptr) { }

    StackPtr(const StackPtr&) = delete;
    StackPtr& operator=(const StackPtr&) = delete;
    StackPtr& operator=(T*) = delete;

    ~StackPtr() {
      if(ptr_ != nullptr) {
        StackPtrFree<T>::free(ptr_);
      }
    }

    T* get() const {
      return ptr_;
    }

    T* release() {
      T* temp = ptr_;
      ptr_ = nullptr;
      return temp;
    }

    void assign(T* ptr) {
      assert(ptr_ == nullptr);
      ptr_ = ptr;
    }

    T** address_of() {
      assert(ptr_ == nullptr);
      return &ptr_;
    }

  private:
    T* ptr_;
};


template<class T>
struct Value;

template<>
struct Value<cass_int32_t> {
    static CassError bind(CassStatement* statement, cass_size_t index, cass_int32_t value) {
      return cass_statement_bind_int32(statement, index, value);
    }

    static CassError get(const CassValue* value, cass_int32_t* output) {
      return cass_value_get_int32(value, output);
    }

    static bool compare(cass_int32_t a, cass_int32_t b) {
      return a == b;
    }
};

template<>
struct Value<cass_int64_t> {
    static CassError bind(CassStatement* statement, cass_size_t index, cass_int64_t value) {
      return cass_statement_bind_int64(statement, index, value);
    }

    static CassError get(const CassValue* value, cass_int64_t* output) {
      return cass_value_get_int64(value, output);
    }

    static bool compare(cass_int64_t a, cass_int64_t b) {
      return a == b;
    }
};

template<>
struct Value<cass_float_t> {
    static CassError bind(CassStatement* statement, cass_size_t index, cass_float_t value) {
      return cass_statement_bind_float(statement, index, value);
    }

    static CassError get(const CassValue* value, cass_float_t* output) {
      return cass_value_get_float(value, output);
    }

    static bool compare(cass_float_t a, cass_float_t b) {
      return a == b;
    }
};

template<>
struct Value<cass_double_t> {
    static CassError bind(CassStatement* statement, cass_size_t index, cass_double_t value) {
      return cass_statement_bind_double(statement, index, value);
    }

    static CassError get(const CassValue* value, cass_double_t* output) {
      return cass_value_get_double(value, output);
    }

    static bool compare(cass_double_t a, cass_double_t b) {
      return a == b;
    }
};

template<>
struct Value<cass_bool_t> {
    static CassError bind(CassStatement* statement, cass_size_t index, cass_bool_t value) {
      return cass_statement_bind_bool(statement, index, value);
    }

    static CassError get(const CassValue* value, cass_bool_t* output) {
      return cass_value_get_bool(value, output);
    }

    static bool compare(cass_bool_t a, cass_bool_t b) {
      return a == b;
    }
};

template<>
struct Value<CassString> {
    static CassError bind(CassStatement* statement, cass_size_t index, CassString value) {
      return cass_statement_bind_string(statement, index, value);
    }

    static CassError get(const CassValue* value, CassString* output) {
      return cass_value_get_string(value, output);
    }

    static bool compare(CassString a, CassString b) {
      if(a.length != b.length) {
        return false;
      }
      return strncmp(a.data, b.data, a.length) == 0;
    }
};

template<>
struct Value<CassBytes> {
    static CassError bind(CassStatement* statement, cass_size_t index, CassBytes value) {
      return cass_statement_bind_bytes(statement, index, value);
    }

    static CassError get(const CassValue* value, CassBytes* output) {
      return cass_value_get_bytes(value, output);
    }

    static bool compare(CassBytes a, CassBytes b) {
      if(a.size != b.size) {
        return false;
      }
      return memcmp(a.data, b.data, a.size) == 0;
    }
};

template<>
struct Value<CassInet> {
    static CassError bind(CassStatement* statement, cass_size_t index, CassInet value) {
      return cass_statement_bind_inet(statement, index, value);
    }

    static CassError get(const CassValue* value, CassInet* output) {
      return cass_value_get_inet(value, output);
    }

    static bool compare(CassInet a, CassInet b) {
      if(a.address_length != b.address_length) {
        return false;
      }
      return memcmp(a.address, b.address, a.address_length) == 0;
    }
};

template<>
struct Value<CassUuid> {
    static CassError bind(CassStatement* statement, cass_size_t index, CassUuid value) {
      return cass_statement_bind_uuid(statement, index, value);
    }

    static CassError get(const CassValue* value, CassUuid* output) {
      return cass_value_get_uuid(value, *output);
    }

    static bool compare(CassUuid a, CassUuid b) {
      return memcmp(a, b, sizeof(CassUuid)) == 0;
    }
};

template<>
struct Value<CassDecimal> {
    static CassError bind(CassStatement* statement, cass_size_t index, CassDecimal value) {
      return cass_statement_bind_decimal(statement, index, value);
    }

    static CassError get(const CassValue* value, CassDecimal* output) {
      return cass_value_get_decimal(value, output);
    }

    static bool compare(CassDecimal a, CassDecimal b) {
      if(a.scale != b.scale) {
        return false;
      }
      if(a.varint.size != b.varint.size) {
        return false;
      }
      return memcmp(a.varint.data, b.varint.data, a.varint.size) == 0;
    }
};



/** The following class cannot be used as a kernel of test fixture because of
    parametrized ctor. Derive from it to use it in your tests.
 */
struct CCM_SETUP {
    CCM_SETUP(int numberOfNodesDC1, int numberOfNodesDC2);
    virtual ~CCM_SETUP();

    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
    const cql::cql_ccm_bridge_configuration_t& conf;

    CassCluster* cluster;
};

const char* get_value_type(CassValueType type);

void execute_query(CassSession* session,
                   const std::string& query,
                   StackPtr<const CassResult>* result = nullptr,
                   cass_size_t parameter_count = 0,
                   CassConsistency consistency = CASS_CONSISTENCY_ONE);

void wait_and_check_error(CassFuture* future, cass_duration_t timeout = 10 * ONE_SECOND_IN_MICROS);

extern const std::string CREATE_KEYSPACE_SIMPLE_FORMAT;
extern const std::string CREATE_KEYSPACE_GENERIC_FORMAT;
extern const std::string SIMPLE_KEYSPACE;
extern const std::string SIMPLE_TABLE;
extern const std::string CREATE_TABLE_SIMPLE_FORMAT;
extern const std::string INSERT_FORMAT;
extern const std::string SELECT_ALL_FORMAT;
extern const std::string SELECT_WHERE_FORMAT;
extern const std::string lorem_ipsum;


} // End of namespace test_utils
