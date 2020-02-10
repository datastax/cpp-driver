##
# Copyright (c) DataStax, Inc.
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

#------------------------------
# Test options
#------------------------------

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
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  if(MSVC_VERSION GREATER 1800 OR MSVC_VERSION EQUAL 1800) # VS2013+/VS 12.0+
    add_definitions(-DGTEST_LANG_CXX11=1)
    add_definitions(-DGTEST_HAS_TR1_TUPLE=0)
  endif()
endif()

#------------------------------
# Google test framework
#------------------------------

# Build the base include paths for the tests
include_directories(${CASS_ROOT_DIR}/tests/src/vendor)
include_directories(${CASS_INCLUDE_DIR})
include_directories(${CASS_SRC_DIR})
include_directories(${CASS_SRC_DIR}/third_party/sparsehash/src)

set(GOOGLE_TEST_DIR ${CASS_ROOT_DIR}/tests/src/vendor/gtest)
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
endif()

#------------------------------
# Compiler flags
#------------------------------

if(CASS_USE_STATIC_LIBS)
  add_definitions(-DCASS_STATIC)
endif()

#------------------------
# Test source files
#------------------------

file(GLOB INTEGRATION_TESTS_INCLUDE_FILES src/integration/*.hpp)
file(GLOB INTEGRATION_TESTS_CCM_INCLUDE_FILES src/integration/ccm/*.hpp)
file(GLOB INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES src/integration/objects/*.hpp)
file(GLOB INTEGRATION_TESTS_POLICIES_INCLUDE_FILES src/integration/policies/*.hpp)
file(GLOB INTEGRATION_TESTS_VALUES_INCLUDE_FILES src/integration/values/*.hpp)
file(GLOB INTEGRATION_TESTS_SOURCE_FILES src/integration/*.cpp)
file(GLOB INTEGRATION_TESTS_CCM_SOURCE_FILES src/integration/ccm/*.cpp)
file(GLOB INTEGRATION_TESTS_OBJECTS_SOURCE_FILES src/integration/objects/*.cpp)
file(GLOB INTEGRATION_TESTS_TESTS_SOURCE_FILES src/integration/tests/*.cpp)

source_group("Header Files" FILES ${INTEGRATION_TESTS_INCLUDE_FILES})
source_group("Header Files\\ccm" FILES ${INTEGRATION_TESTS_CCM_INCLUDE_FILES})
source_group("Header Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES})
source_group("Header Files\\policies" FILES ${INTEGRATION_TESTS_POLICIES_INCLUDE_FILES})
source_group("Header Files\\values" FILES ${INTEGRATION_TESTS_VALUES_INCLUDE_FILES})
source_group("Source Files" FILES ${INTEGRATION_TESTS_SOURCE_FILES})
source_group("Source Files\\ccm" FILES ${INTEGRATION_TESTS_CCM_SOURCE_FILES})
source_group("Source Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES})
source_group("Source Files\\tests" FILES ${INTEGRATION_TESTS_TESTS_SOURCE_FILES})

# Add utility functionality from the Cassandra driver
set(CASS_SRC_DIR ${CASS_ROOT_DIR}/src)
set(CPP_DRIVER_HEADER_SOURCE_FILES ${CASS_SRC_DIR}/atomic.hpp
  ${CASS_SRC_DIR}/macros.hpp
  ${CASS_SRC_DIR}/ref_counted.hpp
  ${CASS_SRC_DIR}/scoped_lock.hpp
  ${CASS_SRC_DIR}/scoped_ptr.hpp
  ${CASS_SRC_DIR}/utils.hpp)
set(CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES ${CASS_SRC_DIR}/atomic/atomic_boost.hpp
  ${CASS_SRC_DIR}/atomic/atomic_intrinsics.hpp
  ${CASS_SRC_DIR}/atomic/atomic_intrinsics_gcc.hpp
  ${CASS_SRC_DIR}/atomic/atomic_intrinsics_msvc.hpp)
set(CPP_DRIVER_SOURCE_FILES ${CASS_SRC_DIR}/utils.cpp)
source_group("Header Files\\driver" FILES ${CASS_API_HEADER_FILES})
source_group("Header Files\\driver" FILES ${CPP_DRIVER_HEADER_SOURCE_FILES})
source_group("Header Files\\driver\\atomic" FILES ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES})
source_group("Source Files\\driver" FILES ${CPP_DRIVER_SOURCE_FILES})

# Group dependencies
source_group("Source Files/dependencies/libuv" FILES ${LIBUV_INCLUDE_FILES})

set(COMMON_INTEGRATION_TEST_INCLUDE_FILES ${INTEGRATION_TESTS_INCLUDE_FILES}
  ${INTEGRATION_TESTS_CCM_INCLUDE_FILES}
  ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES}
  ${INTEGRATION_TESTS_POLICIES_INCLUDE_FILES}
  ${INTEGRATION_TESTS_VALUES_INCLUDE_FILES}
  ${CCM_INCLUDE_FILES}
  ${CASS_API_HEADER_FILES}
  ${CPP_DRIVER_INCLUDE_FILES}
  ${CPP_DRIVER_HEADER_SOURCE_FILES}
  ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES}
  ${GOOGLE_TEST_HEADER_FILES}
  ${LIBUV_INCLUDE_FILES}
  ${LIBSSH2_INCLUDE_FILES}
  ${OPENSSL_INCLUDE_FILES})

set(COMMON_INTEGRATION_TEST_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_FILES}
  ${INTEGRATION_TESTS_CCM_SOURCE_FILES}
  ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES}
  ${INTEGRATION_TESTS_TESTS_SOURCE_FILES}
  ${CPP_DRIVER_SOURCE_FILES}
  ${GOOGLE_TEST_SOURCE_FILES})

#------------------------
# Subdirectories
#------------------------

if(CASS_BUILD_INTEGRATION_TESTS)
  configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR} COPYONLY)
  if(WIN32)
    # Copy the embedded ADS to additional locations for use with IDE
    if(NOT EXISTS ${CMAKE_BINARY_DIR}/tests)
      file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
    endif()
    configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR}/tests COPYONLY)
  endif()

  add_subdirectory(src/integration)
endif()

if(CASS_BUILD_UNIT_TESTS)
  add_subdirectory(src/unit)
endif()
