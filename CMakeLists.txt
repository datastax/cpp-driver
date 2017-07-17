##
# Copyright (c) 2017 DataStax, Inc.
#
# This software can be used solely with DataStax Enterprise. Please consult the
# license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
##

cmake_minimum_required(VERSION 2.6.4)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

# Ensure functions/modules are available
set(CASS_ROOT_DIR "${CMAKE_SOURCE_DIR}/cpp-driver")
set(CASS_SRC_DIR "${CASS_ROOT_DIR}/src")
set(CASS_INCLUDE_DIR "${CASS_ROOT_DIR}/include")
list(APPEND CMAKE_MODULE_PATH ${CASS_ROOT_DIR}/cmake/modules
  ${CMAKE_SOURCE_DIR}/cmake/modules
)

include(CppDriver)

CassInitProject(dse)
CassRapidJson()
CassPolicies()
CassCheckPlatform()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

#------------------------
# Project Version
#------------------------
CassProjectVersion("${PROJECT_SOURCE_DIR}/include/dse.h" "DSE")

#---------------
# Options
#---------------

option(DSE_BUILD_DOCS "Build documentation" OFF)
option(DSE_BUILD_EXAMPLES "Build examples" OFF)
option(DSE_BUILD_INTEGRATION_TESTS "Build the integration tests" OFF)
option(DSE_BUILD_SHARED "Build shared library" ON)
option(DSE_BUILD_STATIC "Build static library" OFF)
option(DSE_BUILD_TESTS "Build tests" OFF)
option(DSE_BUILD_UNIT_TESTS "Build the unit tests" OFF)
option(DSE_DEBUG_CUSTOM_ALLOC "Debug custom allocator" OFF)
option(DSE_INSTALL_HEADER "Install header file" ON)
option(DSE_INSTALL_PKG_CONFIG "Install pkg-config file(s)" ON)
option(DSE_USE_BOOST_ATOMIC "Use Boost atomics library" OFF)
option(DSE_USE_LIBSSH2 "Use libssh2" OFF)
option(DSE_USE_OPENSSL "Use OpenSSL" ON)
option(DSE_USE_SPARSEHASH "Use sparsehash" OFF)
option(DSE_USE_STATIC_LIBS "Link static libraries when building executables" OFF)
option(DSE_USE_STD_ATOMIC "Use C++11 atomics library" OFF)
option(DSE_USE_TCMALLOC "Use tcmalloc" OFF)
option(DSE_USE_ZLIB "Use zlib" OFF)

# Handle testing dependencies
if(DSE_BUILD_TESTS)
  # Enable integration and unit tests
  set(DSE_BUILD_INTEGRATION_TESTS ON)
  set(DSE_BUILD_UNIT_TESTS ON)
endif()

if(DSE_BUILD_INTEGRATION_TESTS)
  # Enable integration tests for core driver
  set(CASS_BUILD_INTEGRATION_TESTS ON)
  set(CASS_USE_OPENSSL ON) # Required for integration tests
endif()

if(DSE_BUILD_UNIT_TESTS)
  # Enable unit tests for core driver
  set(CASS_BUILD_UNIT_TESTS ON)
endif()

if(DSE_BUILD_EXAMPLES)
  # Build examples for core driver
  set(CASS_BUILD_EXAMPLES ON)
endif()

# Determine which driver target should be used as a dependency
set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME})
if(DSE_USE_STATIC_LIBS)
  set(DSE_BUILD_STATIC ON)
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME_STATIC})
endif()

# Set CASS options based on DSE options.
foreach(key USE_STATIC_LIBS USE_BOOST_ATOMIC USE_STD_ATOMIC USE_OPENSSL
            USE_TCMALLOC USE_SPARSEHASH USE_ZLIB USE_LIBSSH2 DEBUG_CUSTOM_ALLOC)
  set(CASS_${key} ${DSE_${key}})
endforeach()

#---------------
# Dependencies
#---------------
CassUseLibuv()

#------------------------
# Optional Dependencies
#------------------------
CassOptionalDependencies()

#----------------------
# Generating API docs
#----------------------
if(DSE_BUILD_DOCS)
  CassDoxygen()
endif()

CassSetCompilerFlags()
CassAddIncludes()
CassFindSourceFiles()
CassConfigure()

source_group("Source Files\\Cassandra" FILES ${CASS_SRC_FILES})
source_group("Header Files\\Cassandra" FILES ${CASS_API_HEADER_FILES} ${CASS_INC_FILES})

# Build up the include paths
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/include)
set(DSE_INCLUDES ${PROJECT_INCLUDE_DIR})
set(DSE_INCLUDES ${DSE_INCLUDES} ${PROJECT_SOURCE_DIR}/src)
set(DSE_INCLUDES ${DSE_INCLUDES} ${CASS_INCLUDES})

# Build up libraries
set(DSE_LIBS ${CASS_LIBS})

# Kerberos
find_package(Kerberos REQUIRED)
set(DSE_INCLUDES ${DSE_INCLUDES} ${KERBEROS_INCLUDE_DIR})
set(DSE_LIBS ${DSE_LIBS} ${KERBEROS_LIBRARIES})

# Build up compiler flags
set(DSE_DRIVER_CXX_FLAGS ${CASS_DRIVER_CXX_FLAGS})
set(TEST_CXX_FLAGS ${CASS_TEST_CXX_FLAGS})

#------------------------------
# DSE static and shared
#------------------------------

# Copy "cassandra.h"
configure_file(${CASS_ROOT_DIR}/include/cassandra.h
               ${PROJECT_SOURCE_DIR}/include/dse/cassandra.h
               COPYONLY)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

file(GLOB DSE_API_HEADER_FILES ${PROJECT_SOURCE_DIR}/include/*.h)
file(GLOB DSE_INC_FILES ${PROJECT_SOURCE_DIR}/src/*.hpp)
file(GLOB DSE_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

source_group("Source Files" FILES ${DSE_SRC_FILES})
source_group("Header Files" FILES ${DSE_API_HEADER_FILES} ${DSE_INC_FILES})
set(DSE_ALL_SOURCE_FILES
  ${DSE_SRC_FILES} ${DSE_API_HEADER_FILES}
  ${DSE_INC_FILES} ${CASS_ALL_SOURCE_FILES})

# Create an object library for the driver (single build)
if(NOT CMAKE_VERSION VERSION_LESS "2.8.8")
  add_library(cpp-driver-dse OBJECT ${DSE_ALL_SOURCE_FILES})
  if(NOT WIN32)
    set_property(TARGET cpp-driver-dse PROPERTY COMPILE_FLAGS "${DSE_DRIVER_CXX_FLAGS} -fPIC")
  endif()
  set_property(TARGET cpp-driver-dse PROPERTY FOLDER "Driver/DSE")

  # Build both shared and static libraries
  set(DSE_BUILD_SHARED ON)
  set(DSE_BUILD_STATIC ON)
endif()

# Assign the include directories
include_directories(${DSE_INCLUDES})
include_directories(${CASS_SRC_DIR})
include_directories(${CASS_INCLUDE_DIR})

# Additional project definitions
add_definitions(-DNOMINMAX)

# Determine if the dynamic/shared library should be built
if(DSE_BUILD_SHARED)
  # It would be great to move this logic into the CassConfigureShared
  # macro, but if you do, something goes wrong and the cmake output
  # warns that policy CMP0042 is violated.
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME} SHARED ${DSE_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME} SHARED $<TARGET_OBJECTS:cpp-driver-dse>)
  endif()
  CassConfigureShared("DSE")
  set_property(TARGET ${PROJECT_LIB_NAME} PROPERTY FOLDER "Driver/DSE")
endif()

# Determine if the static library should be built
if(DSE_BUILD_STATIC)
  # It would be great to move this logic into the CassConfigureStatic
  # macro, but if you do, something goes wrong and the cmake output
  # warns that policy CMP0042 is violated.
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${DSE_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC $<TARGET_OBJECTS:cpp-driver-dse>)
  endif()
  CassConfigureStatic("DSE")

  # Update the test flags to indicate the use of the static library
  if(DSE_USE_STATIC_LIBS)
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} -DDSE_STATIC")
    set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -DCASS_STATIC")
  endif()

  set_property(TARGET ${PROJECT_LIB_NAME_STATIC} PROPERTY FOLDER "Driver/DSE")
endif()

#-----------------------------
# Unit and Integration Tests
#-----------------------------
if (DSE_BUILD_INTEGRATION_TESTS OR DSE_BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if(CASS_BUILD_INTEGRATION_TESTS)
  set(CASS_TEST_LIBS ${Boost_LIBRARIES})
  if(UNIX)
    set(CASS_TEST_LIBS ${CASS_TEST_LIBS} pthread)
  endif()
endif()

CassConfigureTests(${CASS_ROOT_DIR})

#-----------
# Examples
#-----------

# Determine example directories at cmake execution time.
# When a new example dir is added, the user just runs cmake again to pull it in;
# no need to update CMakeLists.txt!

if(DSE_BUILD_EXAMPLES)
  CassBuildExamples("examples")
  CassBuildExamples("cpp-driver/examples")
endif()

#-------------------------------------
# Installation information
#-------------------------------------
CassConfigureInstall(DSE dse)

if(DSE_INSTALL_HEADER)
  install(DIRECTORY include/dse DESTINATION include FILES_MATCHING PATTERN "*.h")
endif()
