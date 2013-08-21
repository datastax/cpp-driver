#
# Install openssl from source
#

if (NOT openssl_NAME)

CMAKE_MINIMUM_REQUIRED(VERSION 2.8.7)

include(ExternalProject)
include(zlib)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # On Mac, 64-bit builds must be manually requested.
    set (SSL_CONFIGURE_COMMAND ./Configure darwin64-x86_64-cc )
else()
    # The config script seems to auto-select the platform correctly on linux
    # It calls ./Configure for us.
    set (SSL_CONFIGURE_COMMAND ./config)
endif()

set(ABBREV "openssl")
set(${ABBREV}_NAME         ${ABBREV})
set(${ABBREV}_INCLUDE_DIRS ${EXT_PREFIX}/include)
set(APP_DEPENDENCIES ${APP_DEPENDENCIES} ${ABBREV})

message("Installing ${openssl_NAME} into ext build area: ${EXT_PREFIX} ...")

ExternalProject_Add(openssl
  DEPENDS zlib
  PREFIX ${EXT_PREFIX}
  URL http://openssl.org/source/openssl-1.0.0e.tar.gz
  URL_MD5 "7040b89c4c58c7a1016c0dfa6e821c86"
  PATCH_COMMAND ""
  CONFIGURE_COMMAND ${SSL_CONFIGURE_COMMAND} zlib no-shared no-zlib-dynamic --prefix=${EXT_PREFIX}
  BUILD_IN_SOURCE 1
  )

set(${ABBREV}_STATIC_LIBRARIES ${EXT_PREFIX}/lib/libcrypto.a ${EXT_PREFIX}/lib/libssl.a)

set_target_properties(${openssl_NAME} PROPERTIES EXCLUDE_FROM_ALL ON)

endif(NOT openssl_NAME)
