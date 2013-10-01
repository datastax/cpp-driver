
#include <cassert>
#include <cstring>

#include "cql/internal/cql_util.hpp"

char* 
cql::safe_strncpy(char* dest, const char* src, const size_t count) {
	assert(count > 0);
	
	strncpy(dest, src, count - 1);
	// null terminate destination
	dest[count - 1] = '\0';

	return dest;
}

bool 
cql::to_ipaddr(const std::string& str, boost::asio::ip::address* const result)
{
     boost::system::error_code err;
     
     boost::asio::ip::address tmp =
            boost::asio::ip::address::from_string(str, err);

    if(err)
        return false;
     
     if(result)
        *result = tmp;
     
     return true;
}

boost::posix_time::ptime 
cql::utc_now() {
    return boost::posix_time::microsec_clock::universal_time();
}

bool
cql::try_acquire_lock(const boost::shared_ptr<boost::atomic_bool>& lock) {
     bool expected = false;
     bool changed = lock->compare_exchange_strong(expected, true, 
                                    boost::memory_order_seq_cst, 
                                    boost::memory_order_seq_cst);

     return (true == changed);
}
