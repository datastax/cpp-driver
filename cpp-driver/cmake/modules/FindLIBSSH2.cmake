# Try to find the libssh2 library; once done this will define:
#
# LIBSSH2_FOUND         - True if libssh2 was found, false otherwise.
# LIBSSH2_INCLUDE_DIRS  - Include directories needed to include libssh2
#                         headers.
# LIBSSH2_LIBRARIES     - Libraries needed to link to libssh2.
# LIBSSH2_VERSION       - The version of libssh2 found.
# LIBSSH2_VERSION_MAJOR - The major version of libssh2.
# LIBSSH2_VERSION_MINOR - The minor version of libssh2.
# LIBSSH2_VERSION_PATCH - The patch version of libssh2.

if(NOT LIBSSH2_INCLUDE_DIRS AND NOT LIBSSH2_LIBRARIES)
  set(_LIBSSH2_ROOT_HINTS ${LIBSSH2_ROOT_DIR} ${LIBSSH2_ROOT}
                          $ENV{LIBSSH2_ROOT_DIR} $ENV{LIBSSH2_ROOT})
  if(NOT WIN32)
    set(_LIBSSH2_ROOT_PATHS "/usr/"
                            "/usr/local/")
  endif()
  set(_LIBSSH2_ROOT_HINTS_AND_PATHS
      HINTS ${_LIBSSH2_ROOT_HINTS}
      PATHS ${_LIBSSH2_ROOT_PATHS})

  find_path(LIBSSH2_INCLUDE_DIR
            NAMES libssh2.h
            ${_LIBSSH2_ROOT_HINTS_AND_PATHS}
            PATH_SUFFIXES include
            NO_DEFAULT_PATH)
  find_library(LIBSSH2_LIBRARY
               NAMES libssh2 ssh2
               ${_LIBSSH2_ROOT_HINTS_AND_PATHS}
               PATH_SUFFIXES lib
               NO_DEFAULT_PATH)
endif()

if(LIBSSH2_INCLUDE_DIR AND EXISTS ${LIBSSH2_INCLUDE_DIR}/libssh2.h)
  set(_LIBSSH2_VERSION_REGEX "^#define[\t ]+LIBSSH2_VERSION_NUM[\t ]+0x[0-9][0-9][0-9][0-9][0-9][0-9].*")
  file(STRINGS "${LIBSSH2_INCLUDE_DIR}/libssh2.h"
       _LIBSSH2_VERSION_STRING
       REGEX "${_LIBSSH2_VERSION_REGEX}")

  string(REGEX REPLACE "^.*LIBSSH2_VERSION_NUM[\t ]+0x([0-9][0-9]).*$"
         "\\1" LIBSSH2_VERSION_MAJOR "${_LIBSSH2_VERSION_STRING}")
  string(REGEX REPLACE "^.*LIBSSH2_VERSION_NUM[\t ]+0x[0-9][0-9]([0-9][0-9]).*$"
         "\\1" LIBSSH2_VERSION_MINOR  "${_LIBSSH2_VERSION_STRING}")
  string(REGEX REPLACE "^.*LIBSSH2_VERSION_NUM[\t ]+0x[0-9][0-9][0-9][0-9]([0-9][0-9]).*$"
         "\\1" LIBSSH2_VERSION_PATCH "${_LIBSSH2_VERSION_STRING}")
  string(REGEX REPLACE "^0(.+)"
         "\\1" LIBSSH2_VERSION_MAJOR "${LIBSSH2_VERSION_MAJOR}")
  string(REGEX REPLACE "^0(.+)"
         "\\1" LIBSSH2_VERSION_MINOR "${LIBSSH2_VERSION_MINOR}")
  string(REGEX REPLACE "^0(.+)"
         "\\1" LIBSSH2_VERSION_PATCH "${LIBSSH2_VERSION_PATCH}")

  set(LIBSSH2_VERSION "${LIBSSH2_VERSION_MAJOR}.${LIBSSH2_VERSION_MINOR}.${LIBSSH2_VERSION_PATCH}")
  message(STATUS "libssh2 version: v${LIBSSH2_VERSION}")
endif()
unset(_LIBSSH2_VERSION_STRING)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBSSH2 DEFAULT_MSG
                                  LIBSSH2_INCLUDE_DIR
                                  LIBSSH2_LIBRARY)
if(LIBSSH2_FOUND)
  set(LIBSSH2_INCLUDE_DIRS ${LIBSSH2_INCLUDE_DIR})
  set(LIBSSH2_LIBRARIES ${LIBSSH2_LIBRARY})
endif()
mark_as_advanced(LIBSSH2_INCLUDE_DIRS
                 LIBSSH2_LIBRARIES
                 LIBSSH2_VERSION
                 LIBSSH2_VERSION_MAJOR
                 LIBSSH2_VERSION_MINOR
                 LIBSSH2_VERSION_PATCH)
