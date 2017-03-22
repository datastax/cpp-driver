##
# Copyright (c) 2017 DataStax, Inc.
#
# This software can be used solely with DataStax Enterprise. Please consult the
# license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
##

# Project options
option(INTEGRATION_VERBOSE_LOGGING "Disable/Enable verbose integration tests console logging" OFF)
if(INTEGRATION_VERBOSE_LOGGING)
  add_definitions(-DINTEGRATION_VERBOSE_LOGGING)
endif()
option(CCM_VERBOSE_LOGGING "Disable/Enable verbose CCM console logging" OFF)
if(CCM_VERBOSE_LOGGING)
  add_definitions(-DCCM_VERBOSE_LOGGING)
endif()
if(WIN32)
  option(USE_VISUAL_LEAK_DETECTOR "Use Visual Leak Detector" OFF)
  if(USE_VISUAL_LEAK_DETECTOR)
    add_definitions(-DUSE_VISUAL_LEAK_DETECTOR)
  endif()
endif()

# Build the base include paths for the tests
set(TESTS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(TESTS_SCASSANDRA_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration/scassandra")
set(TESTS_SCASSANDRA_SERVER_SOURCE_DIR "${TESTS_SCASSANDRA_SOURCE_DIR}/server")
set(VENDOR_DIR "${TESTS_SOURCE_DIR}/vendor")
include_directories(${VENDOR_DIR})

# Add utility functionality from the Cassandra driver
file(GLOB CASSANDRA_API_HEADER_FILES ${CPP_DRIVER_INCLUDE_DIR}/*.h)
set(CPP_DRIVER_HEADER_SOURCE_FILES ${CPP_DRIVER_SOURCE_DIR}/atomic.hpp
  ${CPP_DRIVER_SOURCE_DIR}/macros.hpp
  ${CPP_DRIVER_SOURCE_DIR}/ref_counted.hpp
  ${CPP_DRIVER_SOURCE_DIR}/scoped_lock.hpp
  ${CPP_DRIVER_SOURCE_DIR}/scoped_ptr.hpp
  ${CPP_DRIVER_SOURCE_DIR}/utils.hpp)
set(CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES ${CPP_DRIVER_SOURCE_DIR}/atomic/atomic_boost.hpp
  ${CPP_DRIVER_SOURCE_DIR}/atomic/atomic_intrinsics.hpp
  ${CPP_DRIVER_SOURCE_DIR}/atomic/atomic_intrinsics_gcc.hpp
  ${CPP_DRIVER_SOURCE_DIR}/atomic/atomic_intrinsics_msvc.hpp)
set(CPP_DRIVER_SOURCE_FILES ${CPP_DRIVER_SOURCE_DIR}/utils.cpp)
source_group("Header Files\\driver" FILES ${CASSANDRA_API_HEADER_FILES})
source_group("Header Files\\driver" FILES ${DSE_API_HEADER_FILES})
source_group("Header Files\\driver" FILES ${CPP_DRIVER_HEADER_SOURCE_FILES})
source_group("Header Files\\driver\\atomic" FILES ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES})
source_group("Source Files\\driver" FILES ${CPP_DRIVER_SOURCE_FILES})

# Group dependencies
source_group("Source Files/dependencies/libuv" FILES ${LIBUV_INCLUDE_FILES})

#------------------------------
# Google test framework
#------------------------------
set(GOOGLE_TEST_DIR "${VENDOR_DIR}/gtest")
set(GOOGLE_TEST_HEADER_FILES "${GOOGLE_TEST_DIR}/gtest.h")
set(GOOGLE_TEST_SOURCE_FILES "${GOOGLE_TEST_DIR}/gtest-all.cc")
source_group("Header Files\\gtest" FILES ${GOOGLE_TEST_HEADER_FILES})
source_group("Source Files\\gtest" FILES ${GOOGLE_TEST_SOURCE_FILES})
if(MSVC AND MSVC_VERSION EQUAL 1700)
  # Summary of tuple support for Microsoft Visual Studio:
  # Compiler    version(MS)  version(cmake)  Support
  # ----------  -----------  --------------  -----------------------------
  # <= VS 2010  <= 10        <= 1600         Use Google Tests's own tuple.
  # VS 2012     11           1700            std::tr1::tuple + _VARIADIC_MAX=10
  # VS 2013     12           1800            std::tr1::tuple
  add_definitions(-D_VARIADIC_MAX=10)
else()
  find_package(Threads REQUIRED)
  set(GOOGLE_TEST_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endif()

#------------------------------
# RapidJSON
#------------------------------
file(GLOB RAPIDJSON_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/*.h)
file(GLOB RAPIDJSON_ERROR_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/error/*.h)
file(GLOB RAPIDJSON_INTERNAL_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/internal/*.h)
file(GLOB RAPIDJSON_MSIINTTYPES_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/msiinttypes/*.h)
source_group("Header Files\\rapidjson" FILES ${RAPIDJSON_HEADER_FILES})
source_group("Header Files\\rapidjson\\error" FILES ${RAPIDJSON_ERROR_HEADER_FILES})
source_group("Header Files\\rapidjson\\internal" FILES ${RAPIDJSON_INTERNAL_HEADER_FILES})
source_group("Header Files\\rapidjson\\msiinttypes" FILES ${RAPIDJSON_MSIINTTYPES_HEADER_FILES})
include_directories(${RAPID_JSON_INCLUDE_DIR})

#------------------------------
# Integration test executable
#------------------------------
if(DSE_BUILD_INTEGRATION_TESTS)
  # Determine if SCassandra server can be built (Java is available)
  find_package(Java COMPONENTS Development)
  if(Java_Development_FOUND)
    # Determine if the submodule should be initialized
    set(SCASSANDRA_SERVER_BUILD_SCRIPT "./gradlew")
    if(WIN32)
      set(SCASSANDRA_SERVER_BUILD_SCRIPT "gradlew.bat")
    endif()
    if(NOT EXISTS "${TESTS_SCASSANDRA_SERVER_SOURCE_DIR}/${SCASSANDRA_SERVER_BUILD_SCRIPT}")
      find_package(Git REQUIRED)
      execute_process(COMMAND git submodule update --init --recursive
        WORKING_DIRECTORY ${TESTS_SCASSANDRA_SERVER_SOURCE_DIR}
        OUTPUT_QUIET)
    endif()

    # Build SCassandra server
    file(GLOB SCASSANDRA_SERVER_STANDALONE_JAR
      ${TESTS_SCASSANDRA_SERVER_SOURCE_DIR}/server/build/libs/scassandra-server_*-standalone.jar)
    if(NOT EXISTS ${SCASSANDRA_SERVER_STANDALONE_JAR})
      message(STATUS "Building standalone SCassandra server")
      execute_process(COMMAND ${SCASSANDRA_SERVER_BUILD_SCRIPT} server:fatJar
        WORKING_DIRECTORY ${TESTS_SCASSANDRA_SERVER_SOURCE_DIR}
        OUTPUT_QUIET)
      file(GLOB SCASSANDRA_SERVER_STANDALONE_JAR
        ${TESTS_SCASSANDRA_SERVER_SOURCE_DIR}/server/build/libs/scassandra-server_*-standalone.jar)
    endif()
  endif()

  # Determine if the SCassandra server will be available for the tests
  if(EXISTS ${SCASSANDRA_SERVER_STANDALONE_JAR})
    # Copy the standalone SCassandra server JAR file to output directory
    configure_file(${SCASSANDRA_SERVER_STANDALONE_JAR} ${CMAKE_BINARY_DIR} COPYONLY)
    if(WIN32)
      # Copy SCassandra server to additional locations for use with IDE
      configure_file(${SCASSANDRA_SERVER_STANDALONE_JAR} ${CMAKE_BINARY_DIR}/tests COPYONLY)
      configure_file(${SCASSANDRA_SERVER_STANDALONE_JAR} ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE} COPYONLY)
    endif()
  else()
    message(WARNING "SCassandra Not Found: SCassandra support will be disabled")
  endif()

  # Add CCM bridge functionality from the Cassandra driver
  set(CCM_BRIDGE_DIR ${CASS_SOURCE_DIR}/test/ccm_bridge)
  set(CCM_BRIDGE_SOURCE_DIR ${CCM_BRIDGE_DIR}/src)
  file(GLOB CCM_BRIDGE_HEADER_FILES ${CCM_BRIDGE_SOURCE_DIR}/*.hpp)
  file(GLOB CCM_BRIDGE_SOURCE_FILES ${CCM_BRIDGE_SOURCE_DIR}/*.cpp)
  if(CASS_USE_LIBSSH2)
    if(OPENSSL_FOUND)
      set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/" $ENV{LIBSSH2_ROOT})
      set(LIBSSH2_ROOT ${LIBSSH2_ROOT} ${LIBSSH2_ROOT_DIR} $ENV{LIBSSH2_ROOT_DIR})
      find_package(LIBSSH2)
      if(LIBSSH2_FOUND)
        # Build up the includes and libraries for CCM dependencies
        include_directories(${LIBSSH2_INCLUDE_DIRS})
        include_directories(${OPENSSL_INCLUDE_DIR})
        set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} ${LIBSSH2_LIBRARIES} ${OPENSSL_LIBRARIES})
        if(ZLIB_FOUND)
          set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} ${ZLIB_LIBRARIES})
        endif()
        if(UNIX)
          set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} pthread)
        endif()
        add_definitions(-DCASS_USE_LIBSSH2 -DOPENSSL_CLEANUP)
        file(GLOB LIBSSH2_INCLUDE_FILES ${LIBSSH2_INCLUDE_DIRS}/*.h)
        source_group("Header Files\\ccm_bridge\\libssh2" FILES ${LIBSSH2_INCLUDE_FILES})
      else()
        message(STATUS "libssh2 is Unavailable: Building integration tests without libssh2 support")
      endif()
    else()
      message(STATUS "OpenSSL is Unavailable: Building integration tests without libssh2 support")
    endif()
  endif()
  if(WIN32)
    add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
    set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} wsock32 ws2_32)
  endif()
  include_directories(${CCM_BRIDGE_SOURCE_DIR})
  source_group("Header Files\\ccm_bridge" FILES ${CCM_BRIDGE_HEADER_FILES})
  source_group("Source Files\\ccm_bridge" FILES ${CCM_BRIDGE_SOURCE_FILES})

  set(INTEGRATION_TESTS_NAME "${PROJECT_NAME_STRING}-integration-tests")
  set(INTEGRATION_TESTS_DISPLAY_NAME "Integration Tests")
  set(INTEGRATION_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration")
  file(GLOB INTEGRATION_TESTS_INCLUDE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/*.hpp)
  file(GLOB INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/objects/*.hpp)
  file(GLOB INTEGRATION_TESTS_VALUES_INCLUDE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/values/*.hpp)
  file(GLOB INTEGRATION_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/*.cpp)
  file(GLOB INTEGRATION_TESTS_OBJECTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/objects/*.cpp)
  file(GLOB INTEGRATION_TESTS_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/tests/*.cpp)
  source_group("Header Files" FILES ${INTEGRATION_TESTS_INCLUDE_FILES})
  source_group("Header Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES})
  source_group("Header Files\\values" FILES ${INTEGRATION_TESTS_VALUES_INCLUDE_FILES})
  source_group("Source Files" FILES ${INTEGRATION_TESTS_SOURCE_FILES})
  source_group("Source Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES})
  source_group("Source Files\\tests" FILES ${INTEGRATION_TESTS_TESTS_SOURCE_FILES})
  if (EXISTS ${SCASSANDRA_SERVER_STANDALONE_JAR})
    # Configure the integration tests to use SCassandra
    add_definitions(-DDSE_USE_STANDALONE_SCASSANDRA_SERVER)
    get_filename_component(SCASSANDRA_SERVER_STANDALONE_JAR_BASENAME
      ${SCASSANDRA_SERVER_STANDALONE_JAR} NAME)
    add_definitions(-DSCASSANDRA_SERVER_JAR="${SCASSANDRA_SERVER_STANDALONE_JAR_BASENAME}")
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES ${TESTS_SCASSANDRA_SOURCE_DIR}/*.hpp)
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES ${TESTS_SCASSANDRA_SOURCE_DIR}/*.cpp)
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/tests/scassandra/*.cpp)
    source_group("Header Files\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES})
    source_group("Source Files\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES})
    source_group("Source Files\\tests\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES})
    include_directories(${TESTS_SCASSANDRA_SOURCE_DIR})
  endif()
  add_executable(${INTEGRATION_TESTS_NAME} ${INTEGRATION_TESTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES}
    ${INTEGRATION_TESTS_TESTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES}
    ${CPP_DRIVER_SOURCE_FILES}
    ${INTEGRATION_TESTS_INCLUDE_FILES}
    ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES}
    ${INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES}
    ${INTEGRATION_TESTS_VALUES_INCLUDE_FILES}
    ${CCM_INCLUDE_FILES}
    ${CASSANDRA_API_HEADER_FILES}
    ${DSE_API_HEADER_FILES}
    ${CPP_DRIVER_INCLUDE_FILES}
    ${CPP_DRIVER_HEADER_SOURCE_FILES}
    ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES}
    ${CCM_BRIDGE_HEADER_FILES}
    ${CCM_BRIDGE_SOURCE_FILES}
    ${GOOGLE_TEST_HEADER_FILES}
    ${GOOGLE_TEST_SOURCE_FILES}
    ${LIBUV_INCLUDE_FILES}
    ${LIBSSH2_INCLUDE_FILES}
    ${OPENSSL_INCLUDE_FILES}
    ${RAPIDJSON_HEADER_FILES})
  if(CMAKE_VERSION VERSION_LESS "2.8.11")
    include_directories(${INTEGRATION_TESTS_SOURCE_DIR})
  else()
    target_include_directories(${INTEGRATION_TESTS_NAME} PUBLIC ${INTEGRATION_TESTS_SOURCE_DIR})
  endif()
  target_link_libraries(${INTEGRATION_TESTS_NAME}
    ${CCM_BRIDGE_LIBRARIES}
    ${GOOGLE_TEST_LIBRARIES}
    ${DSE_LIBS}
    ${PROJECT_LIB_NAME_TARGET})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY PROJECT_LABEL ${INTEGRATION_TESTS_DISPLAY_NAME})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY FOLDER "Tests")
  set_property(TARGET ${INTEGRATION_TESTS_NAME} APPEND PROPERTY COMPILE_FLAGS ${TEST_CXX_FLAGS})

  # Copy the ADS JAR file to output directory
  configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR} COPYONLY)
endif()

#------------------------------
# Unit test executable
#------------------------------
if(DSE_BUILD_UNIT_TESTS)
  set(UNIT_TESTS_NAME "${PROJECT_NAME_STRING}-unit-tests")
  set(UNIT_TESTS_DISPLAY_NAME "Unit Tests")
  set(UNIT_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/unit")
  file(GLOB UNIT_TESTS_INCLUDE_FILES ${UNIT_TESTS_SOURCE_DIR}/*.hpp)
  file(GLOB UNIT_TESTS_SOURCE_FILES ${UNIT_TESTS_SOURCE_DIR}/*.cpp)
  file(GLOB UNIT_TESTS_TESTS_SOURCE_FILES ${UNIT_TESTS_SOURCE_DIR}/tests/*.cpp)
  source_group("Header Files" FILES ${UNIT_TESTS_INCLUDE_FILES})
  source_group("Source Files" FILES ${UNIT_TESTS_SOURCE_FILES})
  source_group("Source Files\\tests" FILES ${UNIT_TESTS_TESTS_SOURCE_FILES})
  add_executable(${UNIT_TESTS_NAME} ${UNIT_TESTS_SOURCE_FILES}
    ${UNIT_TESTS_TESTS_SOURCE_FILES}
    ${CPP_DRIVER_SOURCE_FILES}
    ${UNIT_TESTS_INCLUDE_FILES}
    ${CASSANDRA_API_HEADER_FILES}
    ${DSE_API_HEADER_FILES}
    ${CPP_DRIVER_INCLUDE_FILES}
    ${CPP_DRIVER_HEADER_SOURCE_FILES}
    ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES}
    ${GOOGLE_TEST_HEADER_FILES}
    ${GOOGLE_TEST_SOURCE_FILES}
    ${LIBUV_INCLUDE_FILES})
  if(CMAKE_VERSION VERSION_LESS "2.8.11")
    include_directories(${UNIT_TESTS_SOURCE_DIR})
  else()
    target_include_directories(${UNIT_TESTS_NAME} PUBLIC ${UNIT_TESTS_SOURCE_DIR})
  endif()
  target_link_libraries(${UNIT_TESTS_NAME}
    ${GOOGLE_TEST_LIBRARIES}
    ${DSE_LIBS}
    ${PROJECT_LIB_NAME_TARGET})
  set_property(TARGET ${UNIT_TESTS_NAME} PROPERTY PROJECT_LABEL ${UNIT_TESTS_DISPLAY_NAME})
  set_property(TARGET ${UNIT_TESTS_NAME} PROPERTY FOLDER "Tests")
  set_property(TARGET ${UNIT_TESTS_NAME} APPEND PROPERTY COMPILE_FLAGS ${TEST_CXX_FLAGS})

  # Add the unit tests to be executed by ctest (see CMake BUILD_TESTING)
  add_test(${UNIT_TESTS_DISPLAY_NAME} ${UNIT_TESTS_NAME}
    COMMAND ${UNIT_TESTS_NAME})
  set_tests_properties(${UNIT_TESTS_DISPLAY_NAME} PROPERTIES TIMEOUT 5)
endif()
