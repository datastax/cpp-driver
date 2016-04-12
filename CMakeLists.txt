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

cmake_minimum_required(VERSION 2.6.4)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

set(PROJECT_NAME_STRING dse)
set(PROJECT_LIB_NAME ${PROJECT_NAME_STRING})
set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 OLD)
endif()

project(${PROJECT_NAME_STRING} C CXX)

# Title the project and define the versioning
set(PROJECT_DISPLAY_NAME "DataStax C/C++ DSE Driver")
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Retrieve version from header file
set(PROJECT_VERSION_HEADER_FILE "${PROJECT_SOURCE_DIR}/include/dse.h")

file(STRINGS ${PROJECT_VERSION_HEADER_FILE} PROJECT_VERSION_PARTS
  REGEX "^#define[ \t]+DSE_VERSION_(MAJOR|MINOR|PATCH)[ \t]+[0-9]+$")

# Verify version parts
string(REGEX MATCH "DSE_VERSION_MAJOR[ \t]+[0-9]+" PROJECT_VERSION_MAJOR  ${PROJECT_VERSION_PARTS})
string(REGEX MATCH "DSE_VERSION_MINOR[ \t]+[0-9]+" PROJECT_VERSION_MINOR  ${PROJECT_VERSION_PARTS})
string(REGEX MATCH "DSE_VERSION_PATCH[ \t]+[0-9]+" PROJECT_VERSION_PATCH  ${PROJECT_VERSION_PARTS})
if(NOT PROJECT_VERSION_MAJOR OR NOT PROJECT_VERSION_MINOR OR NOT PROJECT_VERSION_PATCH)
  message(FATAL_ERROR "Unable to retrieve project version from ${PROJECT_VERSION_HEADER_FILE}")
endif()

# Extract version numbers
string(REGEX REPLACE "DSE_VERSION_MAJOR[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_MAJOR  ${PROJECT_VERSION_MAJOR})
string(REGEX REPLACE "DSE_VERSION_MINOR[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_MINOR  ${PROJECT_VERSION_MINOR})
string(REGEX REPLACE "DSE_VERSION_PATCH[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_PATCH  ${PROJECT_VERSION_PATCH})
set(PROJECT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})
message(STATUS "Project version: ${PROJECT_VERSION_STRING}")

# Project options
option(DSE_BUILD_SHARED "Build shared library" ON)
option(DSE_BUILD_STATIC "Build static library" OFF)
option(DSE_INSTALL_HEADER "Install header file" ON)
option(DSE_USE_STATIC_LIBS "Link static libraries when building executables" OFF)
option(DSE_BUILD_EXAMPLES "Build examples" OFF)
option(DSE_BUILD_INTEGRATION_TESTS "Build the integration tests" OFF)
option(DSE_BUILD_UNIT_TESTS "Build the unit tests" OFF)

# Build the Cassandra C/C++ driver submodule
SET(CASS_USE_OPENSSL ON CACHE BOOL "Build with OpenSSL support" FORCE)
set(CPP_DRIVER_DIR "${PROJECT_SOURCE_DIR}/cpp-driver")
set(CPP_DRIVER_INCLUDE_DIR "${CPP_DRIVER_DIR}/include")
set(CPP_DRIVER_SOURCE_DIR "${CPP_DRIVER_DIR}/src")
if(NOT EXISTS "${CPP_DRIVER_DIR}/CMakeLists.txt")
    find_package(Git REQUIRED)
    execute_process(COMMAND git submodule update --init --recursive
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_QUIET)
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CPP_DRIVER_DIR}/cmake/modules/")
add_subdirectory(${CPP_DRIVER_DIR})
set_property(TARGET cassandra PROPERTY FOLDER "Driver/Cassandra")
set_property(TARGET cassandra_static PROPERTY FOLDER "Driver/Cassandra")
set_property(TARGET cpp-driver PROPERTY FOLDER "Driver/Cassandra")
set(DSE_LIBS ${CASS_LIBS})

# Build up the include paths
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(DSE_INCLUDES ${DSE_INCLUDES} ${PROJECT_INCLUDE_DIR})
set(DSE_INCLUDES ${DSE_INCLUDES} ${PROJECT_SOURCE_DIR}/src)
set(DSE_INCLUDES ${DSE_INCLUDES} ${LIBUV_INCLUDE_DIR})
set(DSE_INCLUDES ${DSE_INCLUDES} ${OPENSSL_INCLUDE_DIR})

# Determine which driver target should be used as a dependency
if(DSE_USE_STATIC_LIBS)
  set(DSE_BUILD_STATIC ON)
  set(DSE_LIBS ${DSE_LIBS} cassandra_static)
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME_STATIC})
else()
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME})
  set(DSE_LIBS ${DSE_LIBS} cassandra)
endif()

# Kerberos
find_package(Kerberos REQUIRED)
set(DSE_INCLUDES ${DSE_INCLUDES} ${KERBEROS_INCLUDE_DIR})
set(DSE_LIBS ${DSE_LIBS} ${KERBEROS_LIBRARIES})

#------------------------------
# DSE static and shared
#------------------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

file(GLOB DSE_API_HEADER_FILES ${PROJECT_SOURCE_DIR}/include/*.h)
file(GLOB DSE_INC_FILES ${PROJECT_SOURCE_DIR}/src/*.hpp)
file(GLOB DSE_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

source_group("Source Files" FILES ${DSE_SRC_FILES})
source_group("Header Files" FILES ${DSE_API_HEADER_FILES} ${DSE_INC_FILES})
set(DSE_ALL_SOURCE_FILES ${DSE_SRC_FILES} ${DSE_API_HEADER_FILES} ${DSE_INC_FILES})

# Create an object library for the driver (single build)
if(NOT CMAKE_VERSION VERSION_LESS "2.8.8")
  add_library(cpp-driver-dse OBJECT ${DSE_ALL_SOURCE_FILES})
  if(NOT WIN32)
    set_property(TARGET cpp-driver-dse PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -fPIC")
  endif()
  set_property(TARGET cpp-driver-dse PROPERTY FOLDER "Driver/DSE")

  # Build both shared and static libraries
  set(DSE_BUILD_SHARED ON)
  set(DSE_BUILD_STATIC ON)
endif()

# Assign the include directories
include_directories(${DSE_INCLUDES})
include_directories(${CPP_DRIVER_SOURCE_DIR})
include_directories(${CPP_DRIVER_INCLUDE_DIR})

# Additional project definitions
add_definitions(-DNOMINMAX)

# Determine if the dynamic/shared library should be built
if(DSE_BUILD_SHARED)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME} SHARED ${DSE_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME} SHARED $<TARGET_OBJECTS:cpp-driver-dse>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME} ${DSE_LIBS})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES LINK_FLAGS "${PROJECT_CXX_LINKER_FLAGS}")
  set_property(
    TARGET ${PROJECT_LIB_NAME}
    APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -DDSE_BUILDING")
  set_property(TARGET ${PROJECT_LIB_NAME} PROPERTY FOLDER "Driver/DSE")
endif()

# Determine if the static library should be built
if(DSE_BUILD_STATIC)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${DSE_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC $<TARGET_OBJECTS:cpp-driver-dse>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${DSE_LIBS})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_property(
    TARGET ${PROJECT_LIB_NAME_STATIC}
    APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -DDSE_STATIC")
  set_property(TARGET ${PROJECT_LIB_NAME_STATIC} PROPERTY FOLDER "Driver/DSE")

  # Update the test flags to indicate the use of the static library
  if(DSE_USE_STATIC_LIBS)
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} -DDSE_STATIC")
  endif()
endif()

#-----------
# Examples
#-----------

if(DSE_BUILD_EXAMPLES)
  add_subdirectory(examples/plaintext)
  add_subdirectory(examples/gssapi)
endif()

#--------
# Tests
#--------
if (DSE_BUILD_INTEGRATION_TESTS OR DSE_BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

#-------------------------------------
# Directory installation information
#-------------------------------------

# Determine if the library directory needs to be determined
if(NOT DEFINED CMAKE_INSTALL_LIBDIR)
  if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux" AND
      ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr" OR
       "${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local"))
    if(EXISTS "/etc/debian_version")
      set (CMAKE_INSTALL_LIBDIR "lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    elseif(EXISTS "/etc/redhat-release" OR EXISTS "/etc/fedora-release" OR
           EXISTS "/etc/slackware-version" OR EXISTS "/etc/gentoo-release")
      if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set (CMAKE_INSTALL_LIBDIR "lib64")
      else()
        set (CMAKE_INSTALL_LIBDIR "lib")
      endif()
    else()
      set (CMAKE_INSTALL_LIBDIR "lib")
    endif()
  else()
    set (CMAKE_INSTALL_LIBDIR "lib")
  endif()
endif()

# Create a binary directory executable and DLLs (windows only)
set(INSTALL_DLL_EXE_DIR "bin")

#------------------------------
# Install targets
#------------------------------

# Determine if the header should be installed
if(DSE_INSTALL_HEADER)
  install(FILES ${DSE_API_HEADER_FILES} DESTINATION "include")
endif()

# Install the dynamic/shared library
if(DSE_BUILD_SHARED)
  install(TARGETS ${PROJECT_LIB_NAME}
    RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
endif()
