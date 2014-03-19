#ifndef CQL_CONSTANT_RECONNECTION_POLICY_T_HPP_
#define	CQL_CONSTANT_RECONNECTION_POLICY_T_HPP_
	
#include <boost/date_time/posix_time/posix_time.hpp>
#include "cql/policies/cql_reconnection_policy.hpp"
	
namespace cql {
    class cql_constant_reconnection_schedule_t;

    class CQL_EXPORT cql_constant_reconnection_policy_t			
        : public cql_reconnection_policy_t
    {					
    public:			
					
        inline boost::posix_time::time_duration
			base_delay() const { return _base_delay; }
					
        virtual boost::shared_ptr<cql_reconnection_schedule_t>
			new_schedule();
					
        cql_constant_reconnection_policy_t(
            const boost::posix_time::time_duration& base_delay );
					
    private:		
        boost::posix_time::time_duration    _base_delay;
    };				
					


    class CQL_EXPORT cql_constant_reconnection_schedule_t
        : public cql_reconnection_schedule_t
    {
    public:
        virtual boost::posix_time::time_duration
			get_delay();

    private:
        cql_constant_reconnection_schedule_t(
			boost::posix_time::time_duration const & base_delay ) : 
			_base_delay( base_delay ) {}
				
        friend class cql_constant_reconnection_policy_t;
		boost::posix_time::time_duration const    _base_delay;	
    };
}

#endif	/* CQL_CONSTANT_RECONNECTION_POLICY_T_HPP_ */

