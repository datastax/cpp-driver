/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef _CASSANDRA_SAFE_ADVANCE_H_INCLUDE_
#define _CASSANDRA_SAFE_ADVANCE_H_INCLUDE_

namespace cql {
	// Advances iterator diff times (must be positive).
	// Stops when iterator reaches sequence end.
	template<typename TIterator>
	void safe_advance(TIterator& iterator, TIterator& end, size_t diff) {
		while(diff-- > 0) {
			if(iterator == end)
				break;
			++iterator;
		}
	}
}

#endif
