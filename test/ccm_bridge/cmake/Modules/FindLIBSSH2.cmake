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
			PATHS ${LIBSSH2_ROOT} ENV LIBSSH2_ROOT
			PATH_SUFFIXES include
			DOC "LIBSSH include directory")
			
if(WIN32 AND MSVC)
	# In Visual Studio we must link to proper version of library
	find_library(LIBSSH2_LIBRARY_RELEASE
			NAMES libssh2
			PATHS ${LIBSSH2_ROOT} ENV LIBSSH2_ROOT
			PATH_SUFFIXES lib
			DOC "LIBSSH library directory")
	
	find_library(LIBSSH2_LIBRARY_DEBUG
			NAMES libssh2d
			PATHS ${LIBSSH2_ROOT} ENV LIBSSH2_ROOT
			PATH_SUFFIXES lib
			DOC "LIBSSH library directory")
	
	# Select debug or release version of library
	include(SelectLibraryConfigurations)
    select_library_configurations(LIBSSH2)

	set(LIBSSH2_LIBRARIES ${LIBSSH2_LIBRARY})
else()
	find_library(LIBSSH2_LIBRARIES
			NAMES libssh2 ssh2
			PATHS ${LIBSSH2_ROOT} ENV LIBSSH2_ROOT
			PATH_SUFFIXES lib
			DOC "LIBSSH library directory")
endif()

find_package_handle_standard_args(LIBSSH2 DEFAULT_MSG LIBSSH2_LIBRARIES LIBSSH2_INCLUDE_DIRS)
mark_as_advanced(LIBSSH2_INCLUDE_DIRS LIBSSH2_LIBRARIES)
