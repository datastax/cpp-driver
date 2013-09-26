#ifndef CQL_METADATA_H_
#define CQL_METADATA_H_

#include <vector>
#include <list>

#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/noncopyable.hpp>

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


		schema_change_type_enum	_change_type;
		std::string				_keyspace;
		std::string				_table;
	};

    // CURRENTLY THIS IS MOCK THAT IS USED ONLY IN POLICIES
    // TO POPULATE HOSTS COLLECTIONS.
    
    class cql_cluster_impl_t;
    class cql_host_t;
    class cql_hosts_t;
    
	class cql_metadata_t: boost::noncopyable {
	public:
//		typedef
//			boost::signals2::signal<void(const cql_host_state_changed_info_t&)>
//			on_host_state_changed_t;
//
//		inline void 
//		on_host_state_changed(const on_host_state_changed_t::slot_type& slot) {
////			_host_state_changed.connect(slot);
//		}
//		
//		typedef
//			boost::signals2::signal<void(const cql_schema_changed_info_t&)>
//			on_schema_changed_t;
//
//		inline void 
//		on_schema_changed(const on_schema_changed_t::slot_type& slot) {
////			_schema_changed.connect(slot);
//		}
//        
        // Puts all known hosts at the end of @collection.
        void
        get_hosts(std::vector<boost::shared_ptr<cql_host_t> >& collection) const; 
        
        boost::shared_ptr<cql_host_t>
        get_host(const boost::asio::ip::address& ip_address) const;
        
        // Puts host addresses at the end of @collection
        void
        get_host_addresses(std::vector<boost::asio::ip::address>& collection) const;
        
    private:
        
        boost::shared_ptr<cql_host_t>
        add_host(const boost::asio::ip::address& ip_address);
        
        void
        add_contact_points(const std::list<std::string>& contact_points);
        
        void
        remove_host(const boost::asio::ip::address& ip_address);
        
        void
        set_down_host(const boost::asio::ip::address& ip_address);
        
        void
        bring_up_host(const boost::asio::ip::address& ip_address);
        
	private:
        cql_metadata_t(
            boost::shared_ptr<cql_reconnection_policy_t> reconnection_policy);
        
        friend class cql_cluster_pimpl_t;
        
		//on_host_state_changed_t	_host_state_changed;
		//on_schema_changed_t		_schema_changed;
        
        boost::shared_ptr<cql_reconnection_policy_t>    _reconnection_policy;
        boost::shared_ptr<cql_hosts_t>                  _hosts;
	};
}

#endif // CQL_METADATA_H_