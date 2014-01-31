#include "cql/cql.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql_ccm_bridge.hpp"
#include "cql/cql_session.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

#include "test_utils.hpp"

namespace test_utils {
//-----------------------------------------------------------------------------------
// This function is called asynchronously every time an event is logged
void
log_callback(
    const cql::cql_short_t,
    const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}

boost::shared_ptr<cql::cql_result_t>
query(
    boost::shared_ptr<cql::cql_session_t> session,
    std::string query_string,
    cql::cql_consistency_enum cl)
{
    boost::shared_ptr<cql::cql_query_t> _query(
        new cql::cql_query_t(query_string,cl));
    boost::shared_future<cql::cql_future_result_t> query_future = session->query(_query);
    query_future.wait();
    cql::cql_future_result_t query_result = query_future.get();
    return query_result.result;
}

std::string
get_cql(cql::cql_column_type_enum col_type)
{
    static const struct _column_type_to_string {
        _column_type_to_string() {
            CQL_TYPE[cql::CQL_COLUMN_TYPE_CUSTOM] = "custom";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_ASCII] = "ascii";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_BIGINT] = "bigint";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_BLOB] = "blob";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_BOOLEAN] = "boolean";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_COUNTER] = "counter";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_DECIMAL] = "decimal";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_DOUBLE] = "double";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_FLOAT] = "float";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_INT] = "int";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_TEXT] = "text";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_TIMESTAMP] = "timestamp";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_UUID] = "uuid";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_VARCHAR] = "varchar";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_VARINT] = "varint";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_TIMEUUID] = "timeuuid";
            CQL_TYPE[cql::CQL_COLUMN_TYPE_INET] = "inet";
        }
        std::map<cql::cql_column_type_enum, std::string> CQL_TYPE;
    } ctts;
    
    return ctts.CQL_TYPE.at(col_type);
}

void
waitFor(
	boost::asio::ip::address node,
	boost::shared_ptr<cql::cql_cluster_t> cluster,
	int maxTry,
	bool waitForDead,
	bool waitForOut)
{
	boost::this_thread::sleep(boost::posix_time::seconds(60)); // Workaround for now, while events doesn't work properly
}

void 
waitForDownWithWait(
	boost::asio::ip::address node,
	boost::shared_ptr<cql::cql_cluster_t> cluster,
	int waitTime)
{
	boost::this_thread::sleep(boost::posix_time::seconds(60)); // Workaround for now, while events doesn't work properly
}


//-----------------------------------------------------------------------------------
const std::string CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %s }";
const std::string CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE {0} WITH replication = { 'class' : '{1}', {2} }";
const std::string SIMPLE_KEYSPACE = "ks";
const std::string SIMPLE_TABLE = "test";
const std::string CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";
const std::string INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
const std::string SELECT_ALL_FORMAT = "SELECT * FROM {0}";
const std::string SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";
//-----------------------------------------------------------------------------------
CCM_SETUP::CCM_SETUP(int numberOfNodesDC1, int numberOfNodesDC2) : conf(cql::get_ccm_bridge_configuration())
{
    boost::debug::detect_memory_leaks(true);
    ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodesDC1, numberOfNodesDC2);
    ccm_contact_seed = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
    use_ssl=false;
    
    builder = cql::cql_cluster_t::builder();
	if(conf.use_logger())	
	    builder->with_log_callback(&test_utils::log_callback);
    builder->add_contact_point(ccm_contact_seed);
    
    if (use_ssl) {
        builder->with_ssl();
    }
}

CCM_SETUP::~CCM_SETUP()
{
    ccm->remove();
}
//-----------------------------------------------------------------------------------
} // End of namespace test_utils
