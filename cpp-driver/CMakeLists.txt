cmake_minimum_required(VERSION 2.6.4)

# Ensure functions/modules are available
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")

set(PROJECT_NAME_STR cassandra)
set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")

# Title the project and define the versioning
project(${PROJECT_NAME_STR} C CXX)

include(CppDriver)

CassPolicies()
CassCheckPlatform()

#------------------------
# Project Version
#------------------------

# Retrieve version from header file
set(PROJECT_VERSION_HEADER_FILE "${PROJECT_SOURCE_DIR}/include/cassandra.h")

file(STRINGS ${PROJECT_VERSION_HEADER_FILE} PROJECT_VERSION_PARTS
  REGEX "^#define[ \t]+CASS_VERSION_(MAJOR|MINOR|PATCH)[ \t]+[0-9]+$")

# Verify version parts
string(REGEX MATCH "CASS_VERSION_MAJOR[ \t]+[0-9]+" PROJECT_VERSION_MAJOR  ${PROJECT_VERSION_PARTS})
string(REGEX MATCH "CASS_VERSION_MINOR[ \t]+[0-9]+" PROJECT_VERSION_MINOR  ${PROJECT_VERSION_PARTS})
string(REGEX MATCH "CASS_VERSION_PATCH[ \t]+[0-9]+" PROJECT_VERSION_PATCH  ${PROJECT_VERSION_PARTS})

if(NOT PROJECT_VERSION_MAJOR OR NOT PROJECT_VERSION_MINOR OR NOT PROJECT_VERSION_PATCH)
  message(FATAL_ERROR "Unable to retrieve project version from ${PROJECT_VERSION_HEADER_FILE}")
endif()

# Extract version numbers
string(REGEX REPLACE "CASS_VERSION_MAJOR[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_MAJOR  ${PROJECT_VERSION_MAJOR})
string(REGEX REPLACE "CASS_VERSION_MINOR[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_MINOR  ${PROJECT_VERSION_MINOR})
string(REGEX REPLACE "CASS_VERSION_PATCH[ \t]+([0-9]+)" "\\1" PROJECT_VERSION_PATCH  ${PROJECT_VERSION_PATCH})

set(PROJECT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

message(STATUS "Project version: ${PROJECT_VERSION_STRING}")

set(CASS_DRIVER_PROJECT_NAME ${PROJECT_NAME_STR})

#---------------
# Options
#---------------

option(CASS_BUILD_SHARED "Build shared library" ON)
option(CASS_BUILD_STATIC "Build static library" OFF)
option(CASS_BUILD_EXAMPLES "Build examples" OFF)
option(CASS_BUILD_DOCS "Build documentation" OFF)
option(CASS_BUILD_TESTS "Build tests" OFF)
option(CASS_BUILD_INTEGRATION_TESTS "Build integration tests" OFF)
option(CASS_BUILD_UNIT_TESTS "Build unit tests" OFF)
option(CASS_INSTALL_HEADER "Install header file" ON)
option(CASS_INSTALL_PKG_CONFIG "Install pkg-config file(s)" ON)
option(CASS_MULTICORE_COMPILATION "Enable multicore compilation" OFF)
option(CASS_USE_STATIC_LIBS "Link static libraries when building executables" OFF)
option(CASS_USE_BOOST_ATOMIC "Use Boost atomics library" OFF)
option(CASS_USE_STD_ATOMIC "Use C++11 atomics library" OFF)
option(CASS_USE_OPENSSL "Use OpenSSL" ON)
option(CASS_USE_TCMALLOC "Use tcmalloc" OFF)
option(CASS_USE_ZLIB "Use zlib" OFF)
option(CASS_USE_LIBSSH2 "Use libssh2 for integration tests" ON)

# Handle testing dependencies
if(CASS_BUILD_TESTS)
  # Enable integration and unit tests for backwards compatibility
  set(CASS_BUILD_INTEGRATION_TESTS ON)
  set(CASS_BUILD_UNIT_TESTS ON)
endif()
if(CASS_BUILD_INTEGRATION_TESTS)
  set(CASS_USE_OPENSSL ON) # Required for integration tests
endif()
if(CASS_BUILD_UNIT_TESTS)
  set(CASS_BUILD_STATIC ON) # Required for unit tests
endif()

# Determine which driver target should be used as a dependency
set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME})
if(CASS_USE_STATIC_LIBS)
  set(CASS_BUILD_STATIC ON)
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME_STATIC})
endif()

# Ensure the driver is configured to build
if(NOT CASS_BUILD_SHARED AND NOT CASS_BUILD_STATIC)
  message(FATAL_ERROR "Driver is not Configured to Build: Ensure shared and/or static library is enabled")
endif()

#---------------
# Dependencies
#---------------

CassUseLibuv()

#------------------------
# Optional Dependencies
#------------------------

# Boost
if(CASS_USE_BOOST_ATOMIC OR CASS_BUILD_INTEGRATION_TESTS OR CASS_BUILD_UNIT_TESTS)
  CassUseBoost()
endif()

# OpenSSL
if(CASS_USE_OPENSSL)
  CassUseOpenSSL()
endif()

# tcmalloc
if(CASS_USE_TCMALLOC)
  CassUseTcmalloc()
endif()

# zlib
if(CASS_USE_ZLIB)
  CassUseZlib()
endif()

#--------------------
# Test Dependencies
#--------------------

# libssh2
if(CASS_BUILD_INTEGRATION_TESTS)
  if(CASS_USE_LIBSSH2)
    # Setup the root directory for libssh2
    set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/" $ENV{LIBSSH2_ROOT})
    set(LIBSSH2_ROOT ${LIBSSH2_ROOT} ${LIBSSH2_ROOT_DIR} $ENV{LIBSSH2_ROOT_DIR})

    # Discover libssh2
    find_package(LIBSSH2 QUIET)
    if(LIBSSH2_FOUND)
      # Assign test libraries (additional boost and libssh2 dependencies)
      set(CASS_TEST_LIBS ${Boost_LIBRARIES} ${LIBSSH2_LIBRARIES})
      if(UNIX)
        set(CASS_TEST_LIBS ${CASS_TEST_LIBS} pthread)
      endif()
      add_definitions(-DCASS_USE_LIBSSH2)
    else()
      message(STATUS "libssh2 is Unavailable: Building integration tests without libssh2 support")
    endif()
  endif()
endif()

#----------------------
# Generating API docs
#----------------------

# Doxygen
if(CASS_BUILD_DOCS)
  find_package(Doxygen)
  if(DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(docs
      ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generating API documentation with Doxygen" VERBATIM)
  endif()
endif()

#------------------------------
# Cassandra static and shared
#------------------------------

set(CASS_SOURCE_DIR ${PROJECT_SOURCE_DIR})

CassSetCompilerFlags()
CassAddIncludes()
CassFindSourceFiles()
CassConfigure()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Create an object library for the driver (single build)
if(NOT CMAKE_VERSION VERSION_LESS "2.8.8")
  add_library(cpp-driver OBJECT ${CASS_ALL_SOURCE_FILES})
  if(NOT WIN32)
    set_property(TARGET cpp-driver PROPERTY COMPILE_FLAGS "${CASS_DRIVER_CXX_FLAGS} -fPIC")
  endif()

  # Build both shared and static libraries
  set(CASS_BUILD_SHARED ON)
  set(CASS_BUILD_STATIC ON)
endif()

# Assign the include directories
include_directories(${CASS_INCLUDES})

# Determine if the dynamic/shared library should be built
if(CASS_BUILD_SHARED)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME} SHARED ${CASS_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME} SHARED $<TARGET_OBJECTS:cpp-driver>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME} ${CASS_LIBS})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES LINK_FLAGS "${PROJECT_CXX_LINKER_FLAGS}")
  set_property(
    TARGET ${PROJECT_LIB_NAME}
    APPEND PROPERTY COMPILE_FLAGS "${CASS_DRIVER_CXX_FLAGS} -DCASS_BUILDING")
endif()

# Determine if the static library should be built
if(CASS_BUILD_STATIC)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${CASS_ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC $<TARGET_OBJECTS:cpp-driver>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${LIBS})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_property(
    TARGET ${PROJECT_LIB_NAME_STATIC}
    APPEND PROPERTY COMPILE_FLAGS "${CASS_DRIVER_CXX_FLAGS} -DCASS_STATIC")

  # Update the test flags to indicate the use of the static library
  if(CASS_USE_STATIC_LIBS)
    set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -DCASS_STATIC")
  endif()
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
           EXISTS "/etc/slackware-version" OR EXISTS "/etc/gentoo-release" OR
           EXISTS "/etc/os-release")
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

#-----------------------------
# Unit and integration tests
#-----------------------------

# Add the unit and integration tests to the build process
if(CASS_BUILD_UNIT_TESTS)
  # Add the unit test project
  add_subdirectory(test/unit_tests)
endif()
if(CASS_BUILD_INTEGRATION_TESTS)
  # Add CCM bridge as a dependency for integration tests
  add_subdirectory("${PROJECT_SOURCE_DIR}/test/ccm_bridge")
  set(CCM_BRIDGE_INCLUDES "${PROJECT_SOURCE_DIR}/test/ccm_bridge/src")

  # Add the integration test project
  add_subdirectory(test/integration_tests)
endif()

#-----------
# Examples
#-----------

if(CASS_BUILD_EXAMPLES)
  add_subdirectory(examples/async)
  add_subdirectory(examples/auth)
  add_subdirectory(examples/basic)
  add_subdirectory(examples/batch)
  add_subdirectory(examples/bind_by_name)
  add_subdirectory(examples/callbacks)
  add_subdirectory(examples/collections)
  add_subdirectory(examples/date_time)
  add_subdirectory(examples/duration)
  add_subdirectory(examples/logging)
  add_subdirectory(examples/maps)
  add_subdirectory(examples/prepared)
  add_subdirectory(examples/uuids)
  add_subdirectory(examples/schema_meta)
  add_subdirectory(examples/simple)
  add_subdirectory(examples/ssl)
  add_subdirectory(examples/paging)
  add_subdirectory(examples/perf)
  add_subdirectory(examples/named_parameters)
  add_subdirectory(examples/tuple)
  add_subdirectory(examples/udt)
endif()

#------------------
# Install targets
#------------------
if(CASS_INSTALL_PKG_CONFIG)
  if(NOT WIN32)
    find_package(PkgConfig)
    if(PKG_CONFIG_FOUND)
      set(prefix ${CMAKE_INSTALL_PREFIX})
      set(exec_prefix ${CMAKE_INSTALL_PREFIX})
      set(libdir ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR})
      set(includedir ${CMAKE_INSTALL_PREFIX}/include)
      set(version ${PROJECT_VERSION_STRING})
    endif()
  endif()
endif()

# Determine if the header should be installed
if(CASS_INSTALL_HEADER)
  install(FILES ${API_HEADER_FILES} DESTINATION "include")
endif()

# Install the dynamic/shared library
if(CASS_BUILD_SHARED)
  install(TARGETS ${PROJECT_LIB_NAME}
    RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
  if(CASS_INSTALL_PKG_CONFIG)
    if(NOT WIN32)
      if(PKG_CONFIG_FOUND)
        configure_file("${PROJECT_SOURCE_DIR}/packaging/cassandra.pc.in" "cassandra.pc" @ONLY)
        install(FILES "${CMAKE_CURRENT_BINARY_DIR}/cassandra.pc"
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
      endif()
    endif()
  endif()
endif()

# Install the static library
if(CASS_BUILD_STATIC)
  install(TARGETS ${PROJECT_LIB_NAME_STATIC}
    RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
  if(CASS_INSTALL_PKG_CONFIG)
    if(NOT WIN32)
      if(PKG_CONFIG_FOUND)
        configure_file("${PROJECT_SOURCE_DIR}/packaging/cassandra_static.pc.in" "cassandra_static.pc" @ONLY)
        install(FILES "${CMAKE_CURRENT_BINARY_DIR}/cassandra_static.pc"
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
      endif()
    endif()
  endif()
endif()

#-------------------
# Uninstall target
#-------------------

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(UNINSTALL
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

#-----------------------------------
# Build an RPM.
#-----------------------------------
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION_STRING})
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "cpp-cassandra-driver")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "C++ driver for Cassandra")
set(CPACK_PACKAGE_CONTACT "Michael Penick")
set(CPACK_PACKAGE_VENDOR "Datastax")
if(32BIT)
  set(CPACK_RPM_PACKAGE_ARCHITECTURE i686)
else()
  set(CPACK_RPM_PACKAGE_ARCHITECTURE x86_64)
endif()
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CPACK_RPM_PACKAGE_ARCHITECTURE}")
set(CPACK_RPM_PACKAGE_REQUIRES "libuv")
include(CPack)
