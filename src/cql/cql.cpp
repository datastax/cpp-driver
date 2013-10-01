#include <boost/preprocessor.hpp>
#include <boost/shared_ptr.hpp>
#include <cds/init.h>
#include <cds/gc/hp.h>
#include <cds/threading/model.h>

#include "cql/cql.hpp"

#define CASE_STRING_FOR_ENUM(enum_const) \
	case enum_const: return #enum_const

const char*
cql::to_string(const cql_consistency_enum consistency) {
	switch(consistency) {
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ANY);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ONE);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_TWO);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_THREE);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_QUORUM);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ALL);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_LOCAL_QUORUM);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_EACH_QUORUM);
	default:
		return "Unknown " BOOST_PP_STRINGIZE(cql_consistency_enum) " value";
	}
}

const char*
cql::to_string(const cql_host_distance_enum host_distance) {
	switch(host_distance) {
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_LOCAL);
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_IGNORE);
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_REMOTE);
	default:
		return "Unknown " BOOST_PP_STRINGIZE(cql_host_distance_enum) " value";
	}
}

namespace {
    bool cql_library_initialized = false;
    boost::shared_ptr<cds::gc::HP> cql_hp_singleton;
}

void
cql::cql_initialize() {
    if(cql_library_initialized)
        return;
    
    cql_library_initialized = true;
    
    cds::Initialize(0);
    cql_hp_singleton = boost::shared_ptr<cds::gc::HP>(
        new cds::gc::HP());
}

void
cql::cql_terminate() {
    if(!cql_library_initialized)
        return;
    
    cql_library_initialized = false;
    
    cql_hp_singleton.reset();
    cds::Terminate();
}

namespace cql {
struct cql_thread_infrastructure_impl_t {
public:
    // libcds thread garbage collector.
    cds::gc::HP::thread_gc _thread_gc;
};
}

cql::cql_thread_infrastructure_t::cql_thread_infrastructure_t() {
    _this  = new cql_thread_infrastructure_impl_t();
}

cql::cql_thread_infrastructure_t::~cql_thread_infrastructure_t() {
    delete _this;
}
