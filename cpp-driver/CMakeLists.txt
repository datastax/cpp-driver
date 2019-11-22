cmake_minimum_required(VERSION 2.6.4)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

# Ensure functions/modules are available
set(CASS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CASS_SRC_DIR "${CASS_ROOT_DIR}/src")
set(CASS_INCLUDE_DIR "${CASS_ROOT_DIR}/include")
list(APPEND CMAKE_MODULE_PATH ${CASS_ROOT_DIR}/cmake/modules)

include(CppDriver)
include(ClangFormat)

CassInitProject(cassandra)
CassPolicies()
CassCheckPlatform()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

#------------------------
# Project Version
#------------------------
CassProjectVersion("${CASS_INCLUDE_DIR}/cassandra.h" "CASS")

#---------------
# Options
#---------------

option(CASS_BUILD_DOCS "Build documentation" OFF)
option(CASS_BUILD_EXAMPLES "Build examples" OFF)
option(CASS_BUILD_INTEGRATION_TESTS "Build integration tests" OFF)
option(CASS_BUILD_SHARED "Build shared library" ON)
option(CASS_BUILD_STATIC "Build static library" OFF)
option(CASS_BUILD_TESTS "Build tests" OFF)
option(CASS_BUILD_UNIT_TESTS "Build unit tests" OFF)
option(CASS_DEBUG_CUSTOM_ALLOC "Debug custom allocator" OFF)
option(CASS_INSTALL_HEADER "Install header file" ON)
option(CASS_INSTALL_HEADER_IN_SUBDIR "Install header file under 'include/cassandra'" OFF)
option(CASS_INSTALL_PKG_CONFIG "Install pkg-config file(s)" ON)
option(CASS_MULTICORE_COMPILATION "Enable multicore compilation" ON)
option(CASS_USE_BOOST_ATOMIC "Use Boost atomics library" OFF)
option(CASS_USE_KERBEROS "Use Kerberos" OFF)
option(CASS_USE_LIBSSH2 "Use libssh2 for integration tests" OFF)
option(CASS_USE_OPENSSL "Use OpenSSL" ON)
option(CASS_USE_STATIC_LIBS "Link static libraries when building executables" OFF)
option(CASS_USE_STD_ATOMIC "Use C++11 atomics library" OFF)
option(CASS_USE_TCMALLOC "Use tcmalloc" OFF)
option(CASS_USE_ZLIB "Use zlib" ON)
option(CASS_USE_TIMERFD "Use timerfd (Linux only)" ON)

# Handle testing dependencies
if(CASS_BUILD_TESTS)
  # Enable integration and unit tests
  set(CASS_BUILD_INTEGRATION_TESTS ON)
  set(CASS_BUILD_UNIT_TESTS ON)
endif()

if(CASS_BUILD_INTEGRATION_TESTS OR CASS_BUILD_UNIT_TESTS)
  set(CASS_USE_OPENSSL ON) # Required for tests
  set(CASS_USE_KERBEROS ON) # Required for tests
endif()

# Determine which driver target should be used as a dependency
set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME})
if(CASS_USE_STATIC_LIBS OR
   (WIN32 AND (CASS_BUILD_INTEGRATION_TESTS OR CASS_BUILD_UNIT_TESTS)))
  set(CASS_USE_STATIC_LIBS ON) # Not all driver internals are exported for test executable (e.g. CASS_EXPORT)
  set(CASS_BUILD_STATIC ON)
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME_STATIC})
endif()

# Ensure the driver is configured to build
if(NOT CASS_BUILD_SHARED AND NOT CASS_BUILD_STATIC)
  message(FATAL_ERROR "Driver is not Configured to Build: Ensure shared and/or static library is enabled")
endif()

if(CASS_DEBUG_CUSTOM_ALLOC AND CASS_USE_STATIC_LIBS)
  message(WARNING "Debugging the custom allocator while static linking the library can cause your application to fail")
endif()

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

# Doxygen
if(CASS_BUILD_DOCS)
  CassDoxygen()
endif()

#------------------------------
# Cassandra static and shared
#------------------------------

CassSetCompilerFlags()
CassAddIncludes()
CassFindSourceFiles()
CassConfigure()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Assign the include directories
include_directories(${CASS_INCLUDES})

# Determine if the dynamic/shared library should be built
if(CASS_BUILD_SHARED)
  add_library(${PROJECT_LIB_NAME} SHARED ${CASS_ALL_SOURCE_FILES})
  if(CASS_USE_BOOST_ATOMIC AND BOOST_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME} ${BOOST_LIBRARY_NAME})
  endif()
  if(LIBUV_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME} ${LIBUV_LIBRARY_NAME})
  endif()
  if(OPENSSL_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME} ${OPENSSL_LIBRARY_NAME})
  endif()
  CassConfigureShared("CASS")
  set_property(TARGET ${PROJECT_LIB_NAME} PROPERTY FOLDER "Driver/Cassandra")
endif()

# Determine if the static library should be built
if(CASS_BUILD_STATIC)
  add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${CASS_ALL_SOURCE_FILES})
  if(CASS_USE_BOOST_ATOMIC AND BOOST_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME_STATIC} ${BOOST_LIBRARY_NAME})
  endif()
  if(LIBUV_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME_STATIC} ${LIBUV_LIBRARY_NAME})
  endif()
  if(OPENSSL_LIBRARY_NAME)
    add_dependencies(${PROJECT_LIB_NAME_STATIC} ${OPENSSL_LIBRARY_NAME})
  endif()
  CassConfigureStatic("CASS")

  # Set pkg_config required libraries
  set(PC_REQUIRED_LIBS "libuv")
  if (CASS_USE_OPENSSL)
    set(PC_REQUIRED_LIBS "${PC_REQUIRED_LIBS} openssl")
  endif()

  set_property(TARGET ${PROJECT_LIB_NAME_STATIC} PROPERTY FOLDER "Driver/Cassandra")
endif()

#-----------------------------
# Unit and integration tests
#-----------------------------

CassConfigureTests()

#-----------
# Examples
#-----------

# Determine example directories at cmake execution time.
# When a new example dir is added, the user just runs cmake again to pull it in;
# no need to update CMakeLists.txt!

if(CASS_BUILD_EXAMPLES)
  CassBuildExamples("examples")
endif()

#-------------------------------------
# Installation information
#-------------------------------------
CassConfigureInstall(CASS cassandra)

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
