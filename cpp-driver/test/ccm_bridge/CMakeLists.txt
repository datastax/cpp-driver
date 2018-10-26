cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

# Force OLD style of leaving VERSION variables untouched
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 OLD)
endif()

# Title the project
set(PROJECT_CCM_BRIDGE_LIB_NAME CCMBridge)
project(${PROJECT_CCM_BRIDGE_LIB_NAME} C CXX)

# Options
option(CCM_VERBOSE_LOGGING "Disable/Enable CCM verbose console logging" OFF)
if(CCM_VERBOSE_LOGGING)
  add_definitions(-DCCM_VERBOSE_LOGGING)
endif()
option(OPENSSL_CLEANUP "Cleanup memory resources used by OpenSSL" ON)
if(OPENSSL_CLEANUP)
  include_directories(${OPENSSL_INCLUDE_DIR})
  add_definitions(-DOPENSSL_CLEANUP)
endif()

# Constants
set(CCM_BRIDGE_CONFIGURATION_FILE ${PROJECT_SOURCE_DIR}/data/config.txt) # CCM configuration file

# Gather the header and source files
file(GLOB CCM_BRIDGE_INCLUDE_FILES ${PROJECT_SOURCE_DIR}/src/*.hpp)
file(GLOB CCM_BRIDGE_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${CCM_BRIDGE_SRC_FILES})
source_group("Header Files" FILES ${CCM_BRIDGE_INCLUDE_FILES})

if(CASS_USE_LIBSSH2)
  # Allow for boost directory to be specified on the command line
  if(NOT LIBSSH2_ROOT)
    if(EXISTS "${PROJECT_SOURCE_DIR}/lib/libssh2/")
      set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/")
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/build/libs/libssh2/")
      set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/build/libs/libssh2/")
    elseif(DEFINED ENV{LIBSSH2_ROOT} AND
           EXISTS "$ENV{LIBSSH2_ROOT}")
      set(LIBSSH2_ROOT "$ENV{LIBSSH2_ROOT}")
    endif()
  endif()
  if(LIBSSH2_ROOT_DIR)
    if(EXISTS ${LIBSSH2_ROOT_DIR})
      set(LIBSSH2_ROOT ${LIBSSH2_ROOT_DIR})
    endif()
  endif()

  # Check for libssh2 availability (build if necessary on Windows)
  if(WIN32 AND LIBSSH2_VERSION) # Store the current version of libssh2 to prevent corruption
    set(SAVED_LIBSSH2_VERSION ${LIBSSH2_VERSION})
  endif()
  find_package(Libssh2 QUIET)
  if(WIN32 AND NOT LIBSSH2_FOUND)
    message(STATUS "Unable to Locate libssh2: Third party build step will be performed")
    if(SAVED_LIBSSH2_VERSION)
      set(LIBSSH2_VERSION ${SAVED_LIBSSH2_VERSION})
    endif()
    include(ExternalProject-libssh2)
  elseif(NOT LIBSSH2_FOUND)
    message(STATUS "libssh2 is Unavailable: Building integration tests without libssh2 support")
  endif()
endif()
if(LIBSSH2_FOUND OR LIBSSH2_INCLUDE_DIRS)
  # Build up the includes and libraries for CCM dependencies
  set(CCM_BRIDGE_INCLUDES ${PROJECT_SOURCE_DIR}/src ${LIBSSH2_INCLUDE_DIRS})
  set(CCM_BRIDGE_LIBRARIES ${LIBSSH2_LIBRARIES})
  add_definitions(-DCASS_USE_LIBSSH2 -DOPENSSL_CLEANUP)
  file(GLOB LIBSSH2_INCLUDE_FILES ${LIBSSH2_INCLUDE_DIRS}/*.h)
  source_group("Header Files\\ccm_bridge\\libssh2" FILES ${LIBSSH2_INCLUDE_FILES})

  if(WIN32)
    add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
    set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} wsock32 ws2_32)
  elseif(UNIX)
    set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} pthread)
  endif()
endif()
set(CCM_BRIDGE_LIBRARIES ${CCM_BRIDGE_LIBRARIES} CACHE INTERNAL "CCM Bridge Libraries" FORCE)
set(LIBSSH2_FOUND ${LIBSSH2_FOUND} CACHE INTERNAL "Libssh2 found" FORCE)

# Assign the include directories
include_directories(${CCM_BRIDGE_INCLUDES}
                    ${LIBUV_INCLUDE_DIRS})

# Build the CCM bridge static library
add_library(CCMBridge STATIC ${CCM_BRIDGE_SRC_FILES}
                             ${CCM_BRIDGE_INCLUDE_FILES}
                             ${CCM_BRIDGE_CONFIGURATION_FILE}
                             ${LIBSSH2_INCLUDE_FILES})
if(LIBSSH2_LIBRARY_NAME)
  add_dependencies(CCMBridge ${LIBSSH2_LIBRARY_NAME})
  add_definitions(-DLIBSSH2_NO_OPENSSL)
endif()
if(LIBUV_LIBRARY_NAME)
  add_dependencies(CCMBridge ${LIBUV_LIBRARY_NAME})
else()
  add_dependencies(CCMBridge ${LIBUV_LIBRARIES})
endif()

# Copy data file to output directory
configure_file(${CCM_BRIDGE_CONFIGURATION_FILE} ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
