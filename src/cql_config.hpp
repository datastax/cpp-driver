/*
 * File:   cql_config.hpp
 * Author: mc
 *
 * Created on September 11, 2013, 5:33 PM
 */

#ifndef _CQL_CONFIG_HPP_INCLUDE_
#define	_CQL_CONFIG_HPP_INCLUDE_

//restrict it for windows only by now
#ifdef _WIN32
#ifdef cql_EXPORTS
#define CQL_LINK_DYN
#define CQL_EXPORTS
#endif
#endif

// define CQL_LINK_DYN to link dynamically (via .dll or .so)
// with cql library.

#if defined(CQL_LINK_DYN)
#	if defined _WIN32 || defined __CYGWIN__
#		ifdef CQL_EXPORTS
#			ifdef __GNUC__
#				define CQL_EXPORT __attribute__ ((dllexport))
#			else
#				define CQL_EXPORT __declspec(dllexport) // Note: actually gcc seems to also supports this syntax.
#			endif
#		else
#			ifdef __GNUC__
#				define CQL_EXPORT __attribute__ ((dllimport))
#			else
#				define CQL_EXPORT __declspec(dllimport) // Note: actually gcc seems to also supports this syntax.
#			endif
#		endif
#		define CQL_LOCAL
#	else
#		if __GNUC__ >= 4
#			define CQL_EXPORT __attribute__ ((visibility ("default")))
#			define CQL_LOCAL  __attribute__ ((visibility ("hidden")))
#		else
#			define CQL_EXPORT
#			define CQL_LOCAL
#		endif
#	endif
#else
#	define CQL_EXPORT
#	define CQL_LOCAL
#endif

#endif	/* _CQL_CONFIG_HPP_INCLUDE_ */

