include(FindPackageHandleStandardArgs)

# Utilize pkg-config if available to check for modules
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(GSSAPI QUIET krb5-gssapi)
  pkg_check_modules(KERBEROS QUIET krb5)
endif()

# Use pkg-config or attempt to manually locate
if(GSSAPI_FOUND AND KERBEROS_FOUND)
  set(GSSAPI_INCLUDE_DIR ${GSSAPI_INCLUDEDIR})
  mark_as_advanced(GSSAPI_INCLUDEDIR GSSAPI_INCLUDE_DIR GSSAPI_LIBRARIES)
  set(KERBEROS_INCLUDE_DIR ${KERBEROS_INCLUDEDIR})
  mark_as_advanced(KERBEROS_INCLUDEDIR KERBEROS_INCLUDE_DIR KERBEROS_LIBRARIES)
else()
  # Setup the hints and patch for the Kerberos SDK location
  set(_KERBEROS_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/kerberos/")
  set(_KERBEROS_ROOT_HINTS ${KERBEROS_ROOT_DIR} $ENV{KERBEROS_ROOT_DIR}
                           $ENV{KERBEROS_ROOT_DIR}/MIT/Kerberos)
  if(WIN32)
    if(CMAKE_CL_64)
      set(_KERBEROS_SDK_PROGRAMFILES "$ENV{PROGRAMW6432}")
    else()
      set(_PF86 "PROGRAMFILES(X86)")
      set(_KERBEROS_SDK_PROGRAMFILES "$ENV{${_PF86}}")
    endif()
    set(_KERBEROS_SDK "MIT/Kerberos")
    set(_KERBEROS_ROOT_PATHS "${_KERBEROS_ROOT_PATHS}"
        "${_KERBEROS_SDK_PROGRAMFILES}/${_KERBEROS_SDK}")
  endif()
  set(_KERBEROS_ROOT_HINTS_AND_PATHS
      HINTS ${_KERBEROS_ROOT_HINTS}
      PATHS ${_KERBEROS_ROOT_PATHS})

  # Locate GSSAPI
  find_path(GSSAPI_INCLUDE_DIR
            NAMES gssapi/gssapi.h
            HINTS ${_KERBEROS_INCLUDEDIR} ${_KERBEROS_ROOT_HINTS_AND_PATHS}
            PATH_SUFFIXES include)
  find_library(GSSAPI_LIBRARIES
               NAMES gssapi_krb5 gssapi32 gssapi64
               HINTS ${_KERBEROS_LIBDIR} ${_KERBEROS_ROOT_HINTS_AND_PATHS}
               PATH_SUFFIXES lib lib/i386 lib/amd64)
  mark_as_advanced(GSSAPI_INCLUDE_DIR GSSAPI_LIBRARIES)

  # Locate Kerberos
  find_path(KERBEROS_INCLUDE_DIR
            NAMES krb5.h
            HINTS ${_KERBEROS_INCLUDEDIR} ${_KERBEROS_ROOT_HINTS_AND_PATHS}
            PATH_SUFFIXES include)
  find_library(KERBEROS_LIBRARIES
              NAMES krb5 libkrb5 krb5_32 krb5_64
              HINTS ${_KERBEROS_LIBDIR} ${_KERBEROS_ROOT_HINTS_AND_PATHS}
              PATH_SUFFIXES lib lib/i386 lib/amd64)
  mark_as_advanced(KERBEROS_INCLUDE_DIR KERBEROS_LIBRARIES)
endif()

# Set the fail message appropriately for OS
if(NOT WIN32)
    set(_GSSAPI_LIBRARY "gssapi_krb5")
    set(_KERBEROS_LIBRARY "krb5")
else()
  if(CMAKE_CL_64)
    set(_GSSAPI_LIBRARY "gssapi64")
    set(_KERBEROS_LIBRARY "krb5_64")
  else()
    set(_GSSAPI_LIBRARY "gssapi32")
    set(_KERBEROS_LIBRARY "krb5_32")
  endif()
endif()
set(KERBERBOS_FAIL_MESSAGE "Could NOT find ${_GSSAPI_LIBRARY} and/or ${_KERBEROS_LIBRARY}, try to set the path to the Kerberos root folder in the system variable KERBEROS_ROOT_DIR")

# Determine if Kerberos was fully located (GSSAPI dependent)
set(KERBEROS_INCLUDE_DIR ${KERBEROS_INCLUDE_DIR} ${GSSAPI_INCLUDE_DIR})
set(KERBEROS_LIBRARIES ${KERBEROS_LIBRARIES} ${GSSAPI_LIBRARIES})
message(STATUS "Kerberos: ${KERBEROS_INCLUDE_DIR} ${KERBEROS_LIBRARIES}")
find_package_handle_standard_args(Kerberos
    ${KERBERBOS_FAIL_MESSAGE}
    KERBEROS_LIBRARIES
    KERBEROS_INCLUDE_DIR)
