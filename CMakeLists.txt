CMAKE_MINIMUM_REQUIRED(VERSION 2.8.0)
set(PROJECT_NAME_STR cql)
project(${PROJECT_NAME_STR} C CXX)

#-------------------
# The version number
#-------------------

set (cassandra_VERSION_MAJOR 0)
set (cassandra_VERSION_MINOR 7)
set (cassandra_VERSION_PATCH 0)
set (cassandra_VERSION_STRING ${cassandra_VERSION_MAJOR}.${cassandra_VERSION_MINOR}.${cassandra_VERSION_PATCH})

#-------------------
# setup deps
#-------------------
set(EXT_PREFIX ${PROJECT_SOURCE_DIR}/ext)
set(CMAKE_MODULE_PATH ${EXT_PREFIX})

# OS specific lib stuff
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(Boost_USE_STATIC_LIBS ON)
  set(Boost_USE_STATIC_RUNTIME ON)

  include(openssl)
  include(zlib)
  set(LIBS ${LIBS} ${openssl_STATIC_LIBRARIES} )
  set(LIBS ${LIBS} ${zlib_STATIC_LIBRARIES} )
else()
  set(Boost_USE_STATIC_LIBS OFF)
  set(Boost_USE_STATIC_RUNTIME OFF)

  find_package(OpenSSL)
  if (${OPENSSL_FOUND})
    set(LIBS ${LIBS} ${OPENSSL_LIBRARIES} )
  else()
    message(FATAL_ERROR "couldn't find OpenSSL")
  endif()
endif()

# Boost
set(Boost_USE_MULTITHREADED ON)
find_package(Boost 1.54.0 COMPONENTS system thread unit_test_framework)
set(LIBS ${LIBS} ${Boost_LIBRARIES} )

# Threading
find_package(Threads)
set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT} )

add_custom_target (AppDependencies ALL
  DEPENDS ${APP_DEPENDENCIES}
  COMMENT "Installing any missing dependencies.")

# Build up the include pathes
set(INCLUDES ${INCLUDES} ${openssl_INCLUDE_DIRS} )
set(INCLUDES ${INCLUDES} ${zlib_INCLUDE_DIRS} )
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS} )

#-------------------
# cassandra static and shared
#-------------------
# Some 64 bit linux distros use /usr/lib64 for 64 bit libraries.
# on these platforms we must
IF(NOT DEFINED WANT_LIB64)
        # TODO: detect 64-bit build and existance of /usr/lib64 and set to
        # TRUE.
        MESSAGE("WANT_LIB64 unset; assuming normal library directory names")
        SET(WANT_LIB64 FALSE)
ENDIF(NOT DEFINED WANT_LIB64)

IF(WANT_LIB64)
        SET(LIBDIRNAME "lib64")
ELSE(WANT_LIB64)
        SET(LIBDIRNAME "lib")
ENDIF(WANT_LIB64)
MESSAGE("Will install libraries to ${CMAKE_INSTALL_PREFIX}/${LIBDIRNAME}")

set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${LIBDIRNAME})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${LIBDIRNAME})
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)

include_directories(${INCLUDES} ${PROJECT_INCLUDE_DIR} ${PROJECT_SOURCE_DIR}/src)
file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/cassandra/internal/*.cpp ${PROJECT_SOURCE_DIR}/src/cassandra/*.cpp)

add_library(${PROJECT_LIB_NAME} SHARED ${SRC_FILES})
add_library(${PROJECT_LIB_NAME}-static STATIC ${SRC_FILES})

target_link_libraries(${PROJECT_LIB_NAME} ${LIBS})
SET_TARGET_PROPERTIES(${PROJECT_LIB_NAME}-static PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
SET_TARGET_PROPERTIES(${PROJECT_LIB_NAME} PROPERTIES VERSION ${cassandra_VERSION_STRING} SOVERSION ${cassandra_VERSION_MAJOR})

set(PROJECT_COMPILER_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -pedantic")

set_property(
  TARGET ${PROJECT_LIB_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

set_property(
  TARGET ${PROJECT_LIB_NAME}-static
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

#-------------------
# install target
#-------------------

FILE(GLOB INSTALL_HEADERS "${PROJECT_INCLUDE_DIR}/cassandra/*.hpp")
INSTALL(FILES ${INSTALL_HEADERS} DESTINATION include/cassandra)

INSTALL(TARGETS ${PROJECT_LIB_NAME}-static ${PROJECT_LIB_NAME}
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION ${LIBDIRNAME}
  ARCHIVE DESTINATION ${LIBDIRNAME})


#-------------------
# uninstall target
#-------------------

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)


#-------------------
# unit tests
#-------------------
enable_testing()
add_subdirectory(test/unit_tests)

#-------------------
# the demo program
#-------------------

add_subdirectory(demo EXCLUDE_FROM_ALL)
