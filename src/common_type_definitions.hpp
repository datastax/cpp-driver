/*
 * File:   common_type_definitions.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 3:09 PM
 */

#ifndef COMMON_TYPE_DEFINITIONS_HPP_
#define	COMMON_TYPE_DEFINITIONS_HPP_

#include <map>
#include <string>

#include "cql_endpoint.hpp"
#include "cql_uuid.hpp"
#include "cql_connection.hpp"

namespace cql {

    typedef
    std::map<std::string, std::string>
    cql_credentials_t;
}

#endif	/* COMMON_TYPE_DEFINITIONS_HPP_ */
