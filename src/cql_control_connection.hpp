#ifndef CQL_CONTROL_CONNECTION_H_
#define CQL_CONTROL_CONNECTION_H_

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

#include "cql/cql_builder.hpp"
#include "cql/internal/cql_session_impl.hpp"
#include "cql/cql_host.hpp"

#include <string>

namespace cql {

class cql_cluster_t;
class cql_session_t;
class cql_configuration_t;
class cql_connection_t;

class cql_control_connection_t :
        private boost::noncopyable
{
public:
    cql_control_connection_t(
        cql_cluster_t&                         cluster,
        boost::asio::io_service&               io_service,
        boost::shared_ptr<cql_configuration_t> configuration);

    void
    init();

    void
    shutdown(
        int timeout_ms = -1);

    virtual
    ~cql_control_connection_t();

private:

    void
    metadata_hosts_event(
        void*                          sender,
        boost::shared_ptr<cql_event_t> event);

    void
    setup_event_listener();

    void
    refresh_node_list_and_token_map();

    void
    conn_cassandra_event(
        cql_connection_t&,
        cql_event_t*);

    void
    setup_control_connection(
        bool refresh_only = false);

    bool
    refresh_hosts();

    void
    reconnection_callback(
        const boost::system::error_code& err);

    boost::mutex                                   _mutex;
    bool                                           _is_open;
    boost::shared_ptr<cql_session_impl_t>          _session;
    cql_cluster_t&                                 _cluster;
    boost::asio::io_service&                       _io_service;
    boost::shared_ptr<cql_configuration_t>         _configuration;
    boost::asio::deadline_timer                    _timer;
    cql_connection_t::cql_log_callback_t           _log_callback;
    boost::shared_ptr<cql_connection_t>            _active_connection;
    boost::shared_ptr<cql_reconnection_schedule_t> _reconnection_schedule;

    static cql_host_t::ip_address_t
    make_ipv4_address_from_bytes(
        const cql::cql_byte_t* data);

    static std::string
    select_keyspaces_expression()
    {
        return "SELECT * FROM system.schema_keyspaces;";
    }

    static std::string
    select_column_families_expression()
    {
        return "SELECT * FROM system.schema_columnfamilies;";
    }

    static std::string
    select_columns_expression()
    {
        return "SELECT * FROM system.schema_columns;";
    }

    static std::string
    select_peers_expression()
    {
        return "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers;";
    }

    static std::string
    select_local_expression()
    {
       return "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local';";
    }

};

} // namespace cql

#endif // CQL_CONTROL_CONNECTION_H_
