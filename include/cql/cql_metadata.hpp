#ifndef CQL_METADATA_H_
#define CQL_METADATA_H_

#include <boost/asio/ip/address.hpp>
#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>

#include "cql/policies/cql_reconnection_policy.hpp"

namespace cql {
	class cql_host_state_changed_info_t {
	public:
		enum new_host_state_enum {
			NEW_HOST_STATE_UP,
			NEW_HOST_STATE_DOWN
		};

		inline new_host_state_enum
		new_host_state() const { return _new_host_state; }

		inline const boost::asio::ip::address&
		host_address() const { return _ip_addr; }

		inline short
		host_port() const { return _ip_port; }

	private:
		cql_host_state_changed_info_t(
			new_host_state_enum new_host_state,
			boost::asio::ip::address& ip_addr,
			short ip_port)
			:	_new_host_state(new_host_state),
				_ip_addr(ip_addr), 
				_ip_port(ip_port) { }


		new_host_state_enum			_new_host_state;
		boost::asio::ip::address&	_ip_addr;
		short						_ip_port;
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

	private:
		cql_schema_changed_info_t(
			schema_change_type_enum change_type,
			const std::string& keyspace,
			const std::string& table)
			: _change_type(change_type),
			  _keyspace(keyspace), 
			  _table(table) { }

		friend class ::cql::cql_metadata_t;

		schema_change_type_enum	_change_type;
		std::string				_keyspace;
		std::string				_table;
	};

    // CURRENTLY THIS IS MOCK THAT IS USED ONLY IN POLICIES
    // TO POPULATE HOSTS COLLECTIONS.
	class cql_metadata_t: boost::noncopyable {
	public:
		typedef
			boost::signals2::signal<void(const cql_host_state_changed_info_t&)>
			on_host_state_changed_t;

		inline void 
		on_host_state_changed(const on_host_state_changed_t::slot_type& slot) {
			_host_state_changed.connect(slot);
		}
		
		typedef
			boost::signals2::signal<void(const cql_schema_changed_info_t&)>
			on_schema_changed_t;

		inline void 
		on_schema_changed(const on_schema_changed_t::slot_type& slot) {
			_schema_changed.connect(slot);
		}
        
	private:
        cql_metadata(const cql_cluster_t* cluster);
        
        friend class cql_cluster_t;
        
		on_host_state_changed_t	_host_state_changed;
		on_schema_changed_t		_schema_changed;
	};
}

#endif // CQL_METADATA_H_