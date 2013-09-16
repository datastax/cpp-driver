# System must have set LIBCDS_ROOT 
# environmental variable set
# (on linux we also try to guess if library is
# available).
#
# OUTPUT:
# 	LIBCDS_FOUND - is set to YES if library was found
#	LIBCDS_INCLUDE_DIRS - include dirs to libCDS library
#	LIBCDS_LIBRARIES - list of libCDS libraries
#

include(FindPackageHandleStandardArgs)

find_path(LIBCDS_INCLUDE_DIRS
			NAMES cds
			PATHS ENV LIBCDS_ROOT
			PATH_SUFFIXES include
			DOC "LIBCDS include directory")
			
if(WIN32 AND MSVC)
	# In Visual Studio we must link to proper version of library
	find_library(LIBCDS_LIBRARY_RELEASE
			NAMES libcds-x86-vc10
			PATHS ENV LIBCDS_ROOT
			PATH_SUFFIXES lib
			DOC "LIBSSH library directory")
	
	find_library(LIBCDS_LIBRARY_DEBUG
			NAMES libcds-x86-vc10
			PATHS ENV LIBCDS_ROOT
			PATH_SUFFIXES lib
			DOC "LIBCDS library directory")
	
	# Select debug or release version of library
	include(SelectLibraryConfigurations)
    select_library_configurations(LIBCDS)

	set(LIBCDS_LIBRARIES ${LIBCDS_LIBRARY})
else()
	find_library(LIBCDS_LIBRARIES
			NAMES libcds
			PATHS ENV LIBCDS_ROOT
			PATH_SUFFIXES lib
			DOC "LIBCDS library directory")
endif()

			
find_package_handle_standard_args(LIBCDS DEFAULT_MSG LIBCDS_LIBRARIES LIBCDS_INCLUDE_DIRS)
mark_as_advanced(LIBCDS_INCLUDE_DIRS LIBCDS_LIBRARIES)
