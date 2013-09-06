#
# Finds STATIC version of libSSH2 library.
#
# System must have set LIBSSH2_ROOT 
# environmental variable set
# (on linux we also try to guess if library is
# available).
#
# OUTPUT:
# 	LIBSSH2_FOUND - is set to YES if library was found
#	LIBSSH2_INCLUDE_DIRS - include dirs to libSSH2 library
#	LIBSSH2_LIBRARIES - list of libSSH2 libraries
#

include(FindPackageHandleStandardArgs)

find_path(LIBSSH2_INCLUDE_DIRS
			NAMES libssh2.h
			PATHS ENV LIBSSH2_ROOT
			PATH_SUFFIXES include
			DOC "LIBSSH include directory")
			
set(SSH2_LIB_SUFFIX)

if((CMAKE_BUILD_TYPE MATCHES DEBUG) AND MSVC)
	set(SSH2_LIB_SUFFIX "d")
endif()
			
find_library(LIBSSH2_LIBRARIES
			NAMES "libssh2${SSH2_LIB_SUFFIX}" "ssh2${SSH2_LIB_SUFFIX}"
			PATHS ENV LIBSSH2_ROOT
			PATH_SUFFIXES lib
			DOC "LIBSSH library directory")
			
find_package_handle_standard_args(LIBSSH2 DEFAULT_MSG LIBSSH2_LIBRARIES LIBSSH2_INCLUDE_DIRS)
mark_as_advanced(LIBSSH2_INCLUDE_DIRS LIBSSH2_LIBRARIES)
