# Try to find the libuv library; once done this will define
#
# LIBUV_FOUND         - True if libuv was found, false otherwise.
# LIBUV_INCLUDE_DIRS  - Include directories needed to include libuv headers.
# LIBUV_LIBRARIES     - Libraries needed to link to libuv.
# LIBUV_VERSION       - The version of libuv found.
# LIBUV_VERSION_MAJOR - The major version of libuv.
# LIBUV_VERSION_MINOR - The minor version of libuv.
# LIBUV_VERSION_PATCH - The patch version of libuv.

set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR}
                      ENV LIBUV_ROOT_DIR)
if(NOT WIN32)
  set(_LIBUV_ROOT_PATHS "/usr/"
                        "/usr/local/")
endif()
set(_LIBUV_ROOT_HINTS_AND_PATHS
    HINTS ${_LIBUV_ROOT_HINTS}
    PATHS ${_LIBUV_ROOT_PATHS})

find_path(LIBUV_INCLUDE_DIR
  NAMES uv.h
  ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)
find_library(LIBUV_LIBRARY
  NAMES uv_a uv libuv
  ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH)

# Extract version number if possible.
set(_LIBUV_VERSION_REGEX "#[\t ]*define[\t ]+UV_VERSION_(MAJOR|MINOR|PATCH)[\t ]+[0-9]+")
if(LIBUV_INCLUDE_DIR AND
   EXISTS "${LIBUV_INCLUDE_DIR}/uv/version.h")
  file(STRINGS "${LIBUV_INCLUDE_DIR}/uv/version.h"
       _LIBUV_VERSION
       REGEX "${_LIBUV_VERSION_REGEX}")
elseif(LIBUV_INCLUDE_DIR AND
       EXISTS "${LIBUV_INCLUDE_DIR}/uv-version.h")
  file(STRINGS "${LIBUV_INCLUDE_DIR}/uv-version.h"
       _LIBUV_VERSION
       REGEX "${_LIBUV_VERSION_REGEX}")
elseif(LIBUV_INCLUDE_DIR AND
       EXISTS "${LIBUV_INCLUDE_DIR}/uv.h")
  file(STRINGS "${LIBUV_INCLUDE_DIR}/uv.h"
       _LIBUV_VERSION
       REGEX "${_LIBUV_VERSION_REGEX}")
else()
  set(_LIBUV_VERSION "")
endif()

# Complete the extraction from the major, minor, patch components
foreach(a MAJOR MINOR PATCH)
  if(_LIBUV_VERSION MATCHES "#[\t ]*define[\t ]+UV_VERSION_${a}[\t ]+([0-9]+)")
    set(_LIBUV_VERSION_${a} "${CMAKE_MATCH_1}")
  else()
    unset(_LIBUV_VERSION_${a})
  endif()
endforeach()

# Assign the libuv version for the defined components
if(DEFINED _LIBUV_VERSION_MAJOR AND DEFINED _LIBUV_VERSION_MINOR)
  set(LIBUV_VERSION_MAJOR "${_LIBUV_VERSION_MAJOR}")
  set(LIBUV_VERSION_MINOR "${_LIBUV_VERSION_MINOR}")
  set(LIBUV_VERSION "${LIBUV_VERSION_MAJOR}.${LIBUV_VERSION_MINOR}")
  if(DEFINED _LIBUV_VERSION_PATCH)
    set(LIBUV_VERSION_PATCH "${_LIBUV_VERSION_PATCH}")
    set(LIBUV_VERSION "${LIBUV_VERSION}.${LIBUV_VERSION_PATCH}")
  else()
    unset(LIBUV_VERSION_PATCH)
  endif()
  message(STATUS "libuv version: v${LIBUV_VERSION}")
else()
  set(LIBUV_VERSION_MAJOR "")
  set(LIBUV_VERSION_MINOR "")
  set(LIBUV_VERSION_PATCH "")
  set(LIBUV_VERSION "")
endif()
unset(_LIBUV_VERSION_MAJOR)
unset(_LIBUV_VERSION_MINOR)
unset(_LIBUV_VERSION_PATCH)
unset(_LIBUV_VERSION_REGEX)
unset(_LIBUV_VERSION)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBUV DEFAULT_MSG
                                  LIBUV_INCLUDE_DIR
                                  LIBUV_LIBRARY)
if(LIBUV_FOUND)
  set(LIBUV_INCLUDE_DIRS ${LIBUV_INCLUDE_DIR})
  set(LIBUV_LIBRARIES ${LIBUV_LIBRARY})
endif()
mark_as_advanced(LIBUV_INCLUDE_DIRS
                 LIBUV_LIBRARIES
                 LIBUV_VERSION
                 LIBUV_VERSION_MAJOR
                 LIBUV_VERSION_MINOR
                 LIBUV_VERSION_PATCH)
