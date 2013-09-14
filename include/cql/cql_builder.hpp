#ifndef CQL_BUILDER_H_
#define CQL_BUILDER_H_


#include <list>
#include <string>
#include <boost/asio/ssl.hpp>
#include <boost/smart_ptr.hpp>

#include "cql/cql_client.hpp"
#include "cql/cql_load_balancing_policy.hpp"

namespace cql {

class cql_cluster_t;

class cql_client_options_t {
private:
    cql_client_t::cql_log_callback_t _log_callback;

public:
    cql_client_options_t(cql_client_t::cql_log_callback_t log_callback)
        : _log_callback(log_callback) {}

    cql_client_t::cql_log_callback_t get_log_callback() const {
        return _log_callback;
    }
};

class cql_protocol_options_t {
private:
    const std::list<std::string> _contact_points;
    const int _port;
    boost::shared_ptr<boost::asio::ssl::context> _ssl_context;


public:
    cql_protocol_options_t(
            const std::list<std::string>& contact_points, 
            int port, 
            boost::shared_ptr<boost::asio::ssl::context> ssl_context)
        : _contact_points(contact_points), 
          _port(port),
          _ssl_context(ssl_context) { }

    std::list<std::string> get_contact_points() const {
        return _contact_points;
    }
    
    int get_port() const {
        return _port;
    }
    
    boost::shared_ptr<boost::asio::ssl::context> get_ssl_context() const {
        return _ssl_context;
    }
};

class cql_pooling_options_t {
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

public:
    cql_pooling_options_t() {
        _min_simultaneous_requests_for_local = default_min_requests;
        _min_simultaneous_requests_for_remote = default_min_requests;

        _max_simultaneous_requests_for_local = default_max_requests;
        _max_simultaneous_requests_for_remote = default_max_requests;

        _core_connections_for_local = default_core_pool_local;
        _core_connections_for_remote = default_core_pool_remote;

        _max_connections_for_local = default_max_pool_local;
        _max_connections_for_remote = default_max_pool_remote;
    }
    
    int get_min_simultaneous_requests_per_connection_treshold(cql_host_distance_t distance) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            return _min_simultaneous_requests_for_local;
        case CQL_HOST_REMOTE:
            return _min_simultaneous_requests_for_remote;
        default:
            throw std::invalid_argument("Invalid enumeration value.");
        }
    }

    cql_pooling_options_t& set_min_simultaneous_requests_per_connection_treshold(cql_host_distance_t distance, int minSimultaneousRequests) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            _min_simultaneous_requests_for_local = minSimultaneousRequests;
            break;
        case CQL_HOST_REMOTE:
            _min_simultaneous_requests_for_remote = minSimultaneousRequests;
            break;
        default:
            throw std::out_of_range("Cannot set min streams per connection threshold.");
        }
        return *this;
    }

    int get_max_simultaneous_requests_per_connection_treshold(cql_host_distance_t distance) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            return _max_simultaneous_requests_for_local;
        case CQL_HOST_REMOTE:
            return _max_simultaneous_requests_for_remote;
        default:
            throw std::invalid_argument("Invalid enumeration value.");
        }
    }

    cql_pooling_options_t& set_max_simultaneous_requests_per_connection_treshold(cql_host_distance_t distance, int maxSimultaneousRequests) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            _max_simultaneous_requests_for_local = maxSimultaneousRequests;
            break;
        case CQL_HOST_REMOTE:
            _max_simultaneous_requests_for_remote = maxSimultaneousRequests;
            break;
        default:
            throw std::out_of_range("Cannot set max streams per connection threshold");
        }
        return *this;
    }

    int get_core_connections_per_host(cql_host_distance_t distance) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            return _core_connections_for_local;
        case CQL_HOST_REMOTE:
            return _core_connections_for_remote;
        default:
            throw std::invalid_argument("Invalid enumeration value.");
        }
    }

    cql_pooling_options_t& set_core_connections_per_host(cql_host_distance_t distance, int coreConnections) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            _core_connections_for_local = coreConnections;
            break;
        case CQL_HOST_REMOTE:
            _core_connections_for_remote = coreConnections;
            break;
        default:
            throw std::out_of_range("Cannot set core connections per host");
        }
        return *this;
    }

    int get_max_connection_per_host(cql_host_distance_t distance) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            return _max_connections_for_local;
        case CQL_HOST_REMOTE:
            return _max_connections_for_remote;
        default:
            return 0;
        }
    }

    cql_pooling_options_t& set_max_connections_per_host(cql_host_distance_t distance, int maxConnections) {
        switch (distance) {
        case CQL_HOST_LOCAL:
            _max_connections_for_local = maxConnections;
            break;
        case CQL_HOST_REMOTE:
            _max_connections_for_remote = maxConnections;
            break;
        default:
            throw std::out_of_range("Cannot set max connections per host");
        }
        return *this;
    }
};

class cql_policies_t {
private:
    boost::shared_ptr<cql_load_balancing_policy_t> _load_balancing_policy;
public:
    cql_policies_t():_load_balancing_policy(new cql_round_robin_policy_t()) {}
    boost::shared_ptr<cql_load_balancing_policy_t> get_load_balancing_policy() {
        return _load_balancing_policy;
    }
};

class cql_configuration_t {
private:
    cql_client_options_t _client_options;
    cql_protocol_options_t _protocol_options;
    cql_pooling_options_t _pooling_options;
    cql_policies_t _policies;
public:
    cql_configuration_t(const cql_client_options_t& client_options,
                        const cql_protocol_options_t& protocol_options,
                        const cql_pooling_options_t& pooling_options,
                        const cql_policies_t& policies)
        : _client_options(client_options)
        , _protocol_options(protocol_options)
        , _pooling_options(pooling_options)
        , _policies(policies)
    {}

    cql_protocol_options_t& get_protocol_options() {
        return _protocol_options;
    }
    cql_client_options_t& get_client_options() {
        return _client_options;
    }
    cql_pooling_options_t& get_pooling_options() {
        return _pooling_options;
    }
    cql_policies_t& get_policies() {
        return _policies;
    }
};

class cql_initializer_t {
public:
    virtual std::list<std::string> get_contact_points() const = 0;
    virtual boost::shared_ptr<cql_configuration_t> get_configuration() const = 0;
};

class cql_builder_t : public cql_initializer_t, boost::noncopyable {
private:
    std::list<std::string> _contact_points;
    int _port;
    boost::shared_ptr<boost::asio::ssl::context> _ssl_context;
    cql::cql_client_t::cql_log_callback_t _log_callback;

public:

    cql_builder_t() : _port(9042), _log_callback(0) { }

    virtual std::list<std::string> get_contact_points() const {
        return _contact_points;
    }

    virtual boost::shared_ptr<cql_configuration_t> get_configuration() const {
        return boost::shared_ptr<cql_configuration_t>(
                   new cql_configuration_t(
                       cql_client_options_t(_log_callback),
                       cql_protocol_options_t(_contact_points, _port,_ssl_context),
                       cql_pooling_options_t(),
                       cql_policies_t())
               );
    }

    boost::shared_ptr<cql_cluster_t> build();

    int get_port() const {
        return _port;
    }

    cql_builder_t& with_port(int port) {
        _port = port;
        return *this;
    }

    cql_builder_t& with_ssl() {
        boost::shared_ptr<boost::asio::ssl::context> ssl_context(new boost::asio::ssl::context(boost::asio::ssl::context::sslv23));
        _ssl_context = ssl_context;
        return *this;
    }

    cql_builder_t& add_contact_point(const std::string& contact_point) {
        _contact_points.push_back(contact_point);
        return *this;
    }

    cql_builder_t& add_contact_points(const std::list<std::string>& contact_points) {
        _contact_points.insert(_contact_points.end(),contact_points.begin(),contact_points.end());
        return *this;
    }

    cql_builder_t& with_log_callback(cql::cql_client_t::cql_log_callback_t log_callback) {
        _log_callback = log_callback;
        return *this;
    }

};

} // namespace cql

#endif // CQL_BUILDER_H_
