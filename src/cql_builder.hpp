#ifndef CQL_BUILDER_H_
#define CQL_BUILDER_H_


#include <list>
#include <map>
#include <string>
#include <cassert>
#include <boost/asio/ssl.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/asio/ip/address.hpp>

#include "cql/cql_config.hpp"

#include "cql/policies/cql_round_robin_policy.hpp"
#include "cql/policies/cql_exponential_reconnection_policy_t.hpp"
#include "cql/policies/cql_default_retry_policy.hpp"
#include "cql/cql_endpoint.hpp"
#include "common_type_definitions.hpp"

namespace cql {

class cql_cluster_t;

class cql_client_options_t
{
public:
    cql_client_options_t(
        cql_connection_t::cql_log_callback_t log_callback,
		int thread_pool_size) :
        _log_callback(log_callback),
		_thread_pool_size(thread_pool_size)
    {}

    inline cql_connection_t::cql_log_callback_t
    log_callback() const
    {
        return _log_callback;
    }

	inline int thread_pool_size() const
	{
		return _thread_pool_size;
	}

private:
    cql_connection_t::cql_log_callback_t _log_callback;
	int _thread_pool_size;
};

class cql_protocol_options_t
{
public:
    cql_protocol_options_t(
        const std::list<cql_endpoint_t>&             contact_points,
        boost::shared_ptr<boost::asio::ssl::context> ssl_context) :
        _contact_points(contact_points),
        _ssl_context(ssl_context)
    {}

    const std::list<cql_endpoint_t>&
    contact_points() const
    {
        return _contact_points;
    }

    boost::shared_ptr<boost::asio::ssl::context>
    ssl_context() const
    {
        return _ssl_context;
    }

private:
    const std::list<cql_endpoint_t>              _contact_points;
    boost::shared_ptr<boost::asio::ssl::context> _ssl_context;
};

class cql_pooling_options_t
{
public:
    cql_pooling_options_t()
    {
        _min_simultaneous_requests_for_local = default_min_requests;
        _min_simultaneous_requests_for_remote = default_min_requests;

        _max_simultaneous_requests_for_local = default_max_requests;
        _max_simultaneous_requests_for_remote = default_max_requests;

        _core_connections_for_local = default_core_pool_local;
        _core_connections_for_remote = default_core_pool_remote;

        _max_connections_for_local = default_max_pool_local;
        _max_connections_for_remote = default_max_pool_remote;
    }

    int
    min_simultaneous_requests_per_connection_treshold(
        cql_host_distance_enum distance) const
    {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:   return _min_simultaneous_requests_for_local;
        case CQL_HOST_DISTANCE_REMOTE:  return _min_simultaneous_requests_for_remote;
        case CQL_HOST_DISTANCE_IGNORE:  return 0;
        default:
            throw std::invalid_argument("invalid distance value.");
        }
    }

    cql_pooling_options_t&
    set_min_simultaneous_requests_per_connection_treshold(
        cql_host_distance_enum distance,
        int                    min_simultaneous_connections)
    {
        if (min_simultaneous_connections < 0)
            throw std::invalid_argument("min_simultaneous_connections cannot be negative.");

        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:
            _min_simultaneous_requests_for_local = min_simultaneous_connections;
            break;
        case CQL_HOST_DISTANCE_REMOTE:
            _min_simultaneous_requests_for_remote = min_simultaneous_connections;
            break;
        case CQL_HOST_DISTANCE_IGNORE:
            break;
        default:
            throw std::invalid_argument("invalid distance value.");
        }

        return *this;
    }

    int
    max_simultaneous_requests_per_connection_treshold(
        cql_host_distance_enum distance) const
    {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:   return _max_simultaneous_requests_for_local;
        case CQL_HOST_DISTANCE_REMOTE:  return _max_simultaneous_requests_for_remote;
        case CQL_HOST_DISTANCE_IGNORE:  return 0;
        default:
            throw std::invalid_argument("invalid distance value.");
        }
    }

    cql_pooling_options_t&
    set_max_simultaneous_requests_per_connection_treshold(
        cql_host_distance_enum distance,
        int max_simultaneous_requests)
    {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:
            _max_simultaneous_requests_for_local = max_simultaneous_requests;
            break;
        case CQL_HOST_DISTANCE_REMOTE:
            _max_simultaneous_requests_for_remote = max_simultaneous_requests;
            break;
        case CQL_HOST_DISTANCE_IGNORE:
            break;

        default:
            throw std::invalid_argument("Cannot set max streams per connection threshold");
        }

        return *this;
    }

    int
    core_connections_per_host(cql_host_distance_enum distance) const {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:   return _core_connections_for_local;
        case CQL_HOST_DISTANCE_REMOTE:  return _core_connections_for_remote;
        case CQL_HOST_DISTANCE_IGNORE:  return 0;
        default:
            throw std::invalid_argument("invlaid distance value.");
        }
    }

    cql_pooling_options_t&
    set_core_connections_per_host(
        cql_host_distance_enum distance,
        int                    core_connections)
    {
        if (core_connections < 0) {
            throw std::invalid_argument("core_connections cannot be negative.");
        }

        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:
            _core_connections_for_local = core_connections;
            break;
        case CQL_HOST_DISTANCE_REMOTE:
            _core_connections_for_remote = core_connections;
            break;
        case CQL_HOST_DISTANCE_IGNORE:
            break;

        default:
            throw std::invalid_argument("invlaid distance value.");
        }
        return *this;
    }

    int
    max_connection_per_host(
        cql_host_distance_enum distance) const
    {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:   return _max_connections_for_local;
        case CQL_HOST_DISTANCE_REMOTE:  return _max_connections_for_remote;
        case CQL_HOST_DISTANCE_IGNORE:  return 0;
        default:
            throw std::invalid_argument("invlaid distance value.");
        }
    }

    cql_pooling_options_t&
    set_max_connections_per_host(
        cql_host_distance_enum distance,
        int                    max_connections)
    {
        switch (distance) {
        case CQL_HOST_DISTANCE_LOCAL:
            _max_connections_for_local = max_connections;
            break;
        case CQL_HOST_DISTANCE_REMOTE:
            _max_connections_for_remote = max_connections;
            break;
        case CQL_HOST_DISTANCE_IGNORE:
            break;
        default:
            throw std::invalid_argument("invlaid distance value.");
        }
        return *this;
    }

private:
    static const int default_min_requests = 25;
    static const int default_max_requests = 100;

    static const int default_core_pool_local = 2;
    static const int default_core_pool_remote = 1;

    static const int default_max_pool_local = 8;
    static const int default_max_pool_remote = 2;

    int _min_simultaneous_requests_for_local;
    int _min_simultaneous_requests_for_remote;

    int _max_simultaneous_requests_for_local;
    int _max_simultaneous_requests_for_remote;

    int _core_connections_for_local;
    int _core_connections_for_remote;

    int _max_connections_for_local;
    int _max_connections_for_remote;
};

class cql_policies_t {
public:
    cql_policies_t() :
        _load_balancing_policy(new cql_round_robin_policy_t()),
        _reconnection_policy(
            new cql_exponential_reconnection_policy_t(
                boost::posix_time::seconds(1), // base dealy:
                boost::posix_time::minutes(10))), // max delay
        _retry_policy(new cql_default_retry_policy_t())
    {}


    cql_policies_t(
        boost::shared_ptr<cql_load_balancing_policy_t> load_balancing_policy,
        boost::shared_ptr<cql_reconnection_policy_t>   reconnection_policy,
        boost::shared_ptr<cql_retry_policy_t>          retry_policy) :
        _load_balancing_policy(load_balancing_policy),
        _reconnection_policy(reconnection_policy),
        _retry_policy(retry_policy)
    {}

    inline boost::shared_ptr<cql_load_balancing_policy_t>
    load_balancing_policy() const
    {
        return _load_balancing_policy;
    }

    inline boost::shared_ptr<cql_reconnection_policy_t>
    reconnection_policy() const
    {
        return _reconnection_policy;
    }

    inline boost::shared_ptr<cql_retry_policy_t>
    retry_policy() const
    {
        return _retry_policy;
    }

private:
    friend class cql_configuration_t;

    virtual void
    init(cql_cluster_t* cluster)
    {
        assert(cluster != NULL);
        _load_balancing_policy->init(cluster);
    }

    boost::shared_ptr<cql_load_balancing_policy_t>  _load_balancing_policy;
    boost::shared_ptr<cql_reconnection_policy_t>    _reconnection_policy;
    boost::shared_ptr<cql_retry_policy_t>           _retry_policy;
};

class cql_configuration_t
{
public:
    cql_configuration_t(
        boost::shared_ptr<boost::asio::io_service> io_service,
        const cql_client_options_t&                client_options,
        const cql_protocol_options_t&              protocol_options,
        const cql_pooling_options_t&               pooling_options,
        const cql_policies_t&                      policies,
        const cql_credentials_t&                   credentials) :
        _io_service(io_service),
        _client_options(client_options),
        _protocol_options(protocol_options),
        _pooling_options(pooling_options),
        _policies(policies),
        _credentials(credentials)
    {}

    inline const cql_protocol_options_t&
    protocol_options() const
    {
        return _protocol_options;
    }

    inline const cql_client_options_t&
    client_options() const
    {
        return _client_options;
    }

    inline const cql_pooling_options_t&
    pooling_options() const
    {
        return _pooling_options;
    }

    inline const cql_policies_t&
    policies() const
    {
        return _policies;
    }

    inline const cql_credentials_t&
    credentials() const
    {
        return _credentials;
    }

    inline boost::shared_ptr<boost::asio::io_service>
    io_service()
    {
        return _io_service;
    }

private:
    friend class cql_cluster_impl_t;

    void
    init(cql_cluster_t* cluster)
    {
        _policies.init(cluster);
    }

    boost::shared_ptr<boost::asio::io_service> _io_service;
    cql_client_options_t                       _client_options;
    cql_protocol_options_t                     _protocol_options;
    cql_pooling_options_t                      _pooling_options;
    cql_policies_t                             _policies;
    cql_credentials_t                          _credentials;
};

class cql_initializer_t
{
public:
    virtual const std::list<cql_endpoint_t>&
    contact_points() const = 0;

    virtual boost::shared_ptr<cql_configuration_t>
    configuration() = 0;
};

class CQL_EXPORT cql_builder_t :
        public cql_initializer_t,
        boost::noncopyable
{
public:
    static const int DEFAULT_PORT = 9042;

    cql_builder_t() :
        _io_service(new boost::asio::io_service),
        _log_callback(0),
		_thread_pool_size(2)
    {}
    
    virtual ~cql_builder_t() {}

    inline virtual const std::list<cql_endpoint_t>&
    contact_points() const
    {
        return _contact_points;
    }

    inline virtual boost::shared_ptr<cql_configuration_t>
    configuration()
    {
        return boost::shared_ptr<cql_configuration_t>(
                   new cql_configuration_t(
                       _io_service,
					   cql_client_options_t(_log_callback,_thread_pool_size),
                       cql_protocol_options_t(
                           _contact_points,
                           _ssl_context),
                       cql_pooling_options_t(),
                       cql_policies_t(),
                       _credentials));
    }

    boost::shared_ptr<cql_cluster_t>
    build();

    inline cql_builder_t&
    with_ssl()
    {
        boost::shared_ptr<boost::asio::ssl::context> ssl_context(
            new boost::asio::ssl::context(
#if BOOST_VERSION <= 104800
                *_io_service.get(),
#endif
                boost::asio::ssl::context::sslv23));
        _ssl_context = ssl_context;
        return *this;
    }

    cql_builder_t&
    add_contact_point(
        const ::boost::asio::ip::address& address);

    cql_builder_t&
    add_contact_point(
        const ::boost::asio::ip::address& address,
        unsigned short                    port);

    cql_builder_t&
    add_contact_point(
        const cql_endpoint_t& endpoint);

    cql_builder_t&
    add_contact_points(
        const std::list< ::boost::asio::ip::address>& addresses);

    cql_builder_t&
    add_contact_points(
        const std::list< ::boost::asio::ip::address>& addresses,
        unsigned short                                port);

    cql_builder_t&
    add_contact_points(
        const std::list<cql_endpoint_t>& endpoints);

    inline cql_builder_t&
    with_log_callback(
        cql::cql_connection_t::cql_log_callback_t log_callback)
    {
        _log_callback = log_callback;
        return *this;
    }

    cql_builder_t&
    with_credentials(
        const std::string& user_name,
        const std::string& password);

	cql_builder_t&
	set_thread_pool_size(int thread_pool_size)
	{
		_thread_pool_size=thread_pool_size;
		return *this;
	}


private:
    boost::shared_ptr<boost::asio::io_service>   _io_service;
    std::list<cql_endpoint_t>                    _contact_points;
    boost::shared_ptr<boost::asio::ssl::context> _ssl_context;
    cql_connection_t::cql_log_callback_t         _log_callback;
    cql_credentials_t                            _credentials;
	int											 _thread_pool_size;
};

} // namespace cql

#endif // CQL_BUILDER_H_
