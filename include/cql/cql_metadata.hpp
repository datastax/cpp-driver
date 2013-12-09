#ifndef CQL_METADATA_H_
#define CQL_METADATA_H_

#include <vector>
#include <list>

#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/noncopyable.hpp>
#include <boost/signals2/signal.hpp>

#include "cql/policies/cql_reconnection_policy.hpp"
#include "cql/cql_endpoint.hpp"

namespace cql {
	class cql_host_state_changed_info_t {
	public:
		enum new_host_state_enum {
			NEW_HOST_STATE_UP,
			NEW_HOST_STATE_DOWN
		};

		inline new_host_state_enum
		new_state() const { return _new_state; }

        inline const cql_endpoint_t&
        endpoint() const { return _endpoint; }

        static boost::shared_ptr<cql_host_state_changed_info_t>
		make_instance(
            new_host_state_enum     new_host_state,
            const cql_endpoint_t&   endpoint)
        {
            return boost::shared_ptr<cql_host_state_changed_info_t>(
               new cql_host_state_changed_info_t(new_host_state, endpoint));
        }
        
	private:
		cql_host_state_changed_info_t(
            new_host_state_enum     new_host_state,
            const cql_endpoint_t&   endpoint)
            :	_new_state(new_host_state),
                _endpoint(endpoint) { }
        
		new_host_state_enum			_new_state;
        cql_endpoint_t              _endpoint;
	};

	class cql_schema_changed_info_t {
	public:
		enum schema_change_type_enum {
			SCHEMA_CHANGE_TYPE_CREATED,
			SCHEMA_CHANGE_TYPE_DROPPED,
			SCHEMA_CHANGE_TYPE_UPDATED
		};

		schema_change_type_enum
		change_type() const;

		const std::string&
		keyspace() const;

		const std::string&
		table() const;

        static boost::shared_ptr<cql_schema_changed_info_t>
		make_instance(
            schema_change_type_enum change_type,
            const std::string& keyspace,
                      const std::string& table)
        {
            return boost::shared_ptr<cql_schema_changed_info_t>(
                new cql_schema_changed_info_t(change_type, keyspace, table));
        }
        
	private:
		cql_schema_changed_info_t(
            schema_change_type_enum change_type,
            const std::string& keyspace,
            const std::string& table)
            : _change_type(change_type),
              _keyspace(keyspace),
              _table(table) { }
        
		schema_change_type_enum	_change_type;
		std::string				_keyspace;
		std::string				_table;
	};

    // CURRENTLY THIS IS MOCK THAT IS USED ONLY IN POLICIES
    // TO POPULATE HOSTS COLLECTIONS.

    class cql_cluster_impl_t;
    class cql_host_t;
    class cql_hosts_t;

    class cql_token_map_t: boost::noncopyable {
    public:
        
    private:
        
    };

	class cql_metadata_t: boost::noncopyable {
	public:
		typedef
            boost::signals2::signal<void(boost::shared_ptr<cql_host_state_changed_info_t>)>
			on_host_state_changed_t;

		inline void
		on_host_state_changed(const on_host_state_changed_t::slot_type& slot) {
			_host_state_changed.connect(slot);
		}

		typedef
			boost::signals2::signal<void(boost::shared_ptr<cql_schema_changed_info_t>)>
			on_schema_changed_t;

		inline void
		on_schema_changed(const on_schema_changed_t::slot_type& slot) {
			_schema_changed.connect(slot);
		}


        // Puts all known hosts at the end of @collection.
        void
        get_hosts(std::vector<boost::shared_ptr<cql_host_t> >& collection) const;

        boost::shared_ptr<cql_host_t>
        get_host(const cql_endpoint_t& endpoint) const;

        // Puts host addresses at the end of @collection
        void
        get_endpoints(std::vector<cql_endpoint_t>& collection) const;

        friend class cql_control_connection_t;
    private:

        boost::shared_ptr<cql_host_t>
        add_host(const cql_endpoint_t& endpoint);

        void
        add_hosts(const std::list<cql_endpoint_t>& endpoints);

        void
        remove_host(const cql_endpoint_t& endpoint);

        void
        set_down_host(const cql_endpoint_t& endpoint);

        void
        bring_up_host(const cql_endpoint_t& endpoint);

        void
        set_cluster_name(const std::string& new_name);

	private:
        cql_metadata_t(
            boost::shared_ptr<cql_reconnection_policy_t> reconnection_policy);

        friend class cql_cluster_impl_t;

		on_host_state_changed_t                         _host_state_changed;
		on_schema_changed_t                             _schema_changed;

        boost::shared_ptr<cql_reconnection_policy_t>    _reconnection_policy;
        boost::shared_ptr<cql_hosts_t>                  _hosts;
        std::string                                     _cluster_name;
        cql_token_map_t                                 _token_map;
	};
}

#endif // CQL_METADATA_H_
