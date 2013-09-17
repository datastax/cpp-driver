/*
 * File:   cql_config.hpp
 * Author: mc
 *
 * Created on September 11, 2013, 5:33 PM
 */

#ifndef _CQL_CONFIG_HPP_INCLUDE_
#define	_CQL_CONFIG_HPP_INCLUDE_

// define CQL_LINK_DYN to link dynamically (via .dll or .so)
// with cql library.

#if defined(CQL_LINK_DYN)
#	if defined _WIN32 || defined __CYGWIN__
#		ifdef cql_EXPORTS
#			ifdef __GNUC__
#				define DLL_PUBLIC __attribute__ ((dllexport))
#			else
#				define DLL_PUBLIC __declspec(dllexport) // Note: actually gcc seems to also supports this syntax.
#			endif
#		else
#			ifdef __GNUC__
#				define DLL_PUBLIC __attribute__ ((dllimport))
#			else
#				define DLL_PUBLIC __declspec(dllimport) // Note: actually gcc seems to also supports this syntax.
#			endif
#		endif
#		define DLL_LOCAL
#	else
#		if __GNUC__ >= 4
#			define DLL_PUBLIC __attribute__ ((visibility ("default")))
#			define DLL_LOCAL  __attribute__ ((visibility ("hidden")))
#		else
#			define DLL_PUBLIC
#			define DLL_LOCAL
#		endif
#	endif
#else
#	define DLL_PUBLIC
#	define DLL_LOCAL
#endif

#endif	/* _CQL_CONFIG_HPP_INCLUDE_ */

