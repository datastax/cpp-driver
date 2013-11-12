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
#include <cstdlib>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

#include "cql/internal/cql_rand.hpp"

int
cql::cql_rand() {
	static boost::detail::spinlock sl;
	static bool rand_initialized = false;

	boost::lock_guard<boost::detail::spinlock> lock(sl);
	if(!rand_initialized)
		srand((unsigned)time(NULL));

	return rand();
}
