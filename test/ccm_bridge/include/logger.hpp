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

#ifndef CQL_CCM_LOGGER_H_
#define CQL_CCM_LOGGER_H_

#ifdef CQL_NO_BOOST_LOG
#include <iostream>
    enum _cql_loglevel {
        info,
        warning,
        error
    };

    // ACHTUNG: this is obviously not thread safe.
    // Whenever possible, Boost.Log should be used.
    #define CQL_LOG(x) (x==info ? std::cout : std::cerr)
#else
    #include <boost/log/trivial.hpp>
    #define CQL_LOG(x) BOOST_LOG_TRIVIAL(x)
#endif

#endif // CQL_CCM_LOGGER_H_
