/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <cassert>
#include <cstring>

#include "cql_util.hpp"

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
