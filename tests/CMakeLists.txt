##
# Copyright (c) 2014-2016 DataStax
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
# Integration test executable
#------------------------------
if(DSE_BUILD_INTEGRATION_TESTS)
  # Add CCM bridge functionality from the Cassandra driver
  set(CCM_BRIDGE_DIR ${CPP_DRIVER_DIR}/test/ccm_bridge)
  set(CCM_BRIDGE_SOURCE_DIR ${CPP_DRIVER_DIR}/test/ccm_bridge/src)
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
  add_executable(${INTEGRATION_TESTS_NAME} ${INTEGRATION_TESTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_TESTS_SOURCE_FILES}
    ${CPP_DRIVER_SOURCE_FILES}
    ${INTEGRATION_TESTS_INCLUDE_FILES}
    ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES}
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
    ${OPENSSL_INCLUDE_FILES})
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

  # Copy data file to output directory
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

  # Add the unit tests to be execute by ctest (see CMake BUILD_TESTING)
  add_test(${UNIT_TESTS_DISPLAY_NAME} ${UNIT_TESTS_NAME}
    COMMAND ${UNIT_TESTS_NAME})
  set_tests_properties(${UNIT_TESTS_DISPLAY_NAME} PROPERTIES TIMEOUT 5)
endif()
