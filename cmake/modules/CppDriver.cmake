cmake_minimum_required(VERSION 2.6.4)

#-----------
# Includes
#-----------
include(FindPackageHandleStandardArgs)
include(CheckSymbolExists)
include(CheckCXXSourceCompiles)

#------------------------
# CassInitProject
#
# Set some PROJECT_* variables, given the project name.
# Also declare the project itself.
#
# Output: PROJECT_NAME_STRING, PROJECT_LIB_NAME, PROJECT_LIB_NAME_STATIC
#------------------------
macro(CassInitProject project_name)
  set(PROJECT_NAME_STRING ${project_name})
  set(PROJECT_LIB_NAME ${PROJECT_NAME_STRING})
  set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")

  project(${PROJECT_NAME_STRING} C CXX)
endmacro()


#------------------------
# CassExtractHeaderVersion
#
# Read the given 'version header file', looking for version tokens that start
# with 'prefix'. Parse the discovered version and set ${project_name}_VERSION_*
# variables.
#
# Output: ${project_name}_VERSION_MAJOR, ${project_name}_VERSION_MINOR, 
#         ${project_name}_VERSION_PATCH, ${project_name}_VERSION_STRING
#------------------------
macro(CassExtractHeaderVersion project_name version_header_file prefix)
  # Retrieve version from header file
  file(STRINGS ${version_header_file} _VERSION_PARTS
      REGEX "^#define[ \t]+${prefix}_VERSION_(MAJOR|MINOR|PATCH)[ \t]+[0-9]+$")

  foreach(part MAJOR MINOR PATCH)
    string(REGEX MATCH "${prefix}_VERSION_${part}[ \t]+[0-9]+" 
           ${project_name}_VERSION_${part} ${_VERSION_PARTS})
    # Extract version numbers
    if (${project_name}_VERSION_${part})
      string(REGEX REPLACE "${prefix}_VERSION_${part}[ \t]+([0-9]+)" "\\1" 
             ${project_name}_VERSION_${part} ${${project_name}_VERSION_${part}})
    endif()
  endforeach()

  # Verify version parts
  if(NOT ${project_name}_VERSION_MAJOR AND NOT ${project_name}_VERSION_MINOR)
    message(FATAL_ERROR "Unable to retrieve ${project_name} version from ${version_header_file}")
  endif()

  set(${project_name}_VERSION_STRING 
      ${${project_name}_VERSION_MAJOR}.${${project_name}_VERSION_MINOR})
  if(NOT ${project_name}_VERSION_PATCH STREQUAL "")
    set(${project_name}_VERSION_STRING 
        "${${project_name}_VERSION_STRING}.${${project_name}_VERSION_PATCH}")
  endif()

  message(STATUS "${project_name} version: ${${project_name}_VERSION_STRING}")
endmacro()

#------------------------
# CassProjectVersion
#
# Read the given 'version header file', looking for version tokens that start
# with 'prefix'. Parse the discovered version and set PROJECT_VERSION_*
# variables.
#
# Output: PROJECT_VERSION_MAJOR, PROJECT_VERSION_MINOR, PROJECT_VERSION_PATCH,
#         PROJECT_VERSION_STRING
#------------------------
macro(CassProjectVersion version_header_file prefix)
  CassExtractHeaderVersion("PROJECT" ${version_header_file} ${prefix})
endmacro()

#------------------------
# CassOptionalDependencies
#
# Configure enabled optional dependencies.
#
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassOptionalDependencies)
  # Boost
  if(CASS_USE_BOOST_ATOMIC OR CASS_BUILD_INTEGRATION_TESTS)
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
endmacro()

#------------------------
# CassDoxygen
#
# Configure docs target
#------------------------
macro(CassDoxygen)
  find_package(Doxygen)
  if(DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(docs
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM)
  endif()
endmacro()

#------------------------
# CassConfigureShared
#
# Configure the project to build a shared library.
#
# Arguments:
#    prefix - prefix of global variable names that contain specific
#        info on building the library (e.g. CASS or DSE).
# Input: PROJECT_LIB_NAME, PROJECT_VERSION_STRING, PROJECT_VERSION_MAJOR,
#        PROJECT_CXX_LINKER_FLAGS, *_DRIVER_CXX_FLAGS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassConfigureShared prefix)
  target_link_libraries(${PROJECT_LIB_NAME} ${${prefix}_LIBS})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES LINK_FLAGS "${PROJECT_CXX_LINKER_FLAGS}")
  set_property(
      TARGET ${PROJECT_LIB_NAME}
      APPEND PROPERTY COMPILE_FLAGS "${${prefix}_DRIVER_CXX_FLAGS} -DCASS_BUILDING")
endmacro()

#------------------------
# CassConfigureStatic
#
# Configure the project to build a static library.
#
# Arguments:
#    prefix - prefix of global variable names that contain specific
#        info on building the library (e.g. CASS or DSE).
# Input: PROJECT_LIB_NAME_STATIC, PROJECT_VERSION_STRING, PROJECT_VERSION_MAJOR,
#        PROJECT_CXX_LINKER_FLAGS, *_DRIVER_CXX_FLAGS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassConfigureStatic prefix)
  target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${${prefix}_LIBS})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES LINK_FLAGS "${PROJECT_CXX_LINKER_FLAGS}")
  set_property(
      TARGET ${PROJECT_LIB_NAME_STATIC}
      APPEND PROPERTY COMPILE_FLAGS "${${prefix}_DRIVER_CXX_FLAGS} -DCASS_STATIC")
endmacro()

#------------------------
# CassConfigureExamples
#
# Configure the project to build examples.
#
# Arguments:
#    examples_dir - directory containing all example sub-directories.
macro(CassBuildExamples examples_dir)
  file(GLOB EXAMPLES_TO_BUILD "${examples_dir}/*/CMakeLists.txt")
  foreach(example ${EXAMPLES_TO_BUILD})
    get_filename_component(exdir ${example} PATH)
    add_subdirectory(${exdir})
  endforeach()
endmacro()

#------------------------
# CassConfigureInstall
#
# Configure the project to be able to "install" (e.g. make install).
#
# Arguments:
#    var_prefix - prefix of install-related CMake variables.
#    pkg_config_stem - base name of package config files for this project.
# Input: CMAKE_CURRENT_BINARY_DIR, CMAKE_SYSTEM_NAME, CMAKE_INSTALL_PREFIX,
#        CMAKE_LIBRARY_ARCHITECTURE, *_API_HEADER_FILES, *_BUILD_SHARED, *_BUILD_STATIC,
#        *_INSTALL_HEADER, *_INSTALL_PKG_CONFIG, PROJECT_LIB_NAME, PROJECT_LIB_NAME_STATIC,
#        PROJECT_SOURCE_DIR, PROJECT_VERSION_STRING
# Output: CMAKE_INSTALL_LIBDIR, INSTALL_DLL_EXE_DIR
macro(CassConfigureInstall var_prefix pkg_config_stem)
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

  if(${var_prefix}_INSTALL_PKG_CONFIG)
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
  if(${var_prefix}_INSTALL_HEADER)
    install(FILES ${${var_prefix}_API_HEADER_FILES} DESTINATION "include")
  endif()

  # Install the dynamic/shared library
  if(${var_prefix}_BUILD_SHARED)
    install(TARGETS ${PROJECT_LIB_NAME}
        RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
    if(${var_prefix}_INSTALL_PKG_CONFIG)
      if(NOT WIN32)
        if(PKG_CONFIG_FOUND)
          configure_file("${PROJECT_SOURCE_DIR}/packaging/${pkg_config_stem}.pc.in" "${pkg_config_stem}.pc" @ONLY)
          install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${pkg_config_stem}.pc"
              DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
        endif()
      endif()
    endif()
  endif()

  if(${var_prefix}_BUILD_STATIC)
    install(TARGETS ${PROJECT_LIB_NAME_STATIC}
        RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
    if(${var_prefix}_INSTALL_PKG_CONFIG)
      if(NOT WIN32)
        if(PKG_CONFIG_FOUND)
          configure_file("${PROJECT_SOURCE_DIR}/packaging/${pkg_config_stem}_static.pc.in" "${pkg_config_stem}_static.pc" @ONLY)
          install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${pkg_config_stem}_static.pc"
              DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
        endif()
      endif()
    endif()
  endif()
endmacro()


#-----------
# Policies
#-----------

#------------------------
# CassPolicies
#
# Tweak some CMake behaviors.
#------------------------
macro(CassPolicies)
  # TODO: Figure out Mac OS X rpath
  if(POLICY CMP0042)
    cmake_policy(SET CMP0042 OLD)
  endif()

  # Force OLD style of project versioning variables
  if(POLICY CMP0048)
    cmake_policy(SET CMP0048 OLD)
  endif()
endmacro()

#------------------------
# CassCheckPlatform
#
# Check to ensure the platform is valid for the driver
#------------------------
macro(CassCheckPlatform)
  # Ensure Windows platform is supported
  if(WIN32)
    if(CMAKE_SYSTEM_VERSION GREATER 5.2 OR
       CMAKE_SYSTEM_VERSION EQUAL 5.2)
      add_definitions(-D_WIN32_WINNT=0x502)
    else()
      string(REPLACE "." "" WINDOWS_VERSION ${CMAKE_SYSTEM_VERSION})
      string(REGEX REPLACE "([0-9])" "0\\1" WINDOWS_VERSION ${WINDOWS_VERSION})
      message(FATAL_ERROR "Unable to build driver: Unsupported Windows platform 0x${WINDOWS_VERSION}")
    endif()
  endif()
endmacro()

#------------------------
# CassConfigureTests
#
# Add test subdirs for core driver to the build if testing is enabled.
#
# Input: CASS_BUILD_INTEGRATION_TESTS, CASS_BUILD_UNIT_TESTS, CASS_ROOT_DIR
#------------------------
macro(CassConfigureTests)
  if(CASS_BUILD_INTEGRATION_TESTS)
    # Add CCM bridge as a dependency for integration tests
    set(CCM_BRIDGE_INCLUDES "${CASS_ROOT_DIR}/test/ccm_bridge/src")
    add_subdirectory(${CASS_ROOT_DIR}/test/ccm_bridge)
    add_subdirectory(${CASS_ROOT_DIR}/test/integration_tests)
  endif()

  if (CASS_BUILD_INTEGRATION_TESTS OR CASS_BUILD_UNIT_TESTS)
    add_subdirectory(${CASS_ROOT_DIR}/gtests)
  endif()
endmacro()

#---------------
# Dependencies
#---------------

#------------------------
# CassUseLibuv
#
# Add includes and  libraries required for using libuv.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseLibuv)
  # Setup the paths and hints for libuv
  set(_LIBUV_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/libuv/")
  set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR} $ENV{LIBUV_ROOT_DIR})
  if(NOT WIN32)
    set(_LIBUV_ROOT_PATHS "${_LIBUV_ROOT_PATHS}" "/usr/" "/usr/local/")
  endif()
  set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS
    HINTS ${_LIBUV_ROOT_HINTS}
    PATHS ${_LIBUV_ROOT_PATHS})

  # Ensure libuv was found
  find_path(LIBUV_INCLUDE_DIR
    NAMES uv.h
    HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
    PATH_SUFFIXES include)
  find_library(LIBUV_LIBRARY
    NAMES uv libuv
    HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
    PATH_SUFFIXES lib)
  find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
    LIBUV_LIBRARY
    LIBUV_INCLUDE_DIR)

  if (EXISTS "${LIBUV_INCLUDE_DIR}/uv-version.h")
    set(LIBUV_VERSION_HEADER_FILE "${LIBUV_INCLUDE_DIR}/uv-version.h")
  else()
    set(LIBUV_VERSION_HEADER_FILE "${LIBUV_INCLUDE_DIR}/uv.h")
  endif()

  CassExtractHeaderVersion("LIBUV" ${LIBUV_VERSION_HEADER_FILE} "UV")

  if (LIBUV_VERSION_STRING VERSION_LESS "1.0")
    message(WARNING "Libuv version ${LIBUV_VERSION_STRING} does not support reverse "
    "name lookup. Hostname resolution will not work (version 1.0 or greater "
    "required)")
  endif()

  if (LIBUV_VERSION_STRING VERSION_LESS "1.6")
    message(WARNING "Libuv version ${LIBUV_VERSION_STRING} does not support custom "
    "memory allocators (version 1.6 or greater required)")
  endif()

  # Assign libuv include and libraries
  set(CASS_INCLUDES ${CASS_INCLUDES} ${LIBUV_INCLUDE_DIR})
  set(CASS_LIBS ${CASS_LIBS} ${LIBUV_LIBRARY})
endmacro()

#------------------------
# Optional Dependencies
#------------------------

# Minimum supported version of Boost
set(CASS_MINIMUM_BOOST_VERSION 1.59.0)

#------------------------
# CassCcmBridge
#
# Add includes and libraries needed for CCM Bridge.
#
# Input: CASS_ROOT_DIR, CASS_USE_LIBSSH2, OPENSSL_FOUND, OPENSSL_LIBRARIES,
#        PROJECT_SOURCE_DIR, ZLIB_FOUND, ZLIB_LIBRARIES
# Output: CCM_BRIDGE_DIR, CCM_BRIDGE_HEADER_FILES, CCM_BRIDGE_LIBRARIES,
#         CCM_BRIDGE_SOURCE_DIR, CCM_BRIDGE_SOURCE_FILES, LIBSSH2_FOUND
#------------------------
macro(CassCcmBridge)
  # Add CCM bridge functionality from the Core driver
  set(CCM_BRIDGE_DIR ${CASS_ROOT_DIR}/test/ccm_bridge)
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
endmacro()

#------------------------
# CassRapidJson
#
# Set some RAPID_JSON_* variables, set up some source_group's,
# and add the RapidJson include dir to our list of include dirs.
#
# Input: CASS_SRC_DIR
# Output: RAPID_JSON_INCLUDE_DIR, RAPIDJSON_HEADER_FILES, RAPIDJSON_ERROR_HEADER_FILES
#         RAPIDJSON_INTERNAL_HEADER_FILES, RAPIDJSON_MSIINTTYPES_HEADER_FILES
#------------------------
macro(CassRapidJson)
  set(RAPID_JSON_INCLUDE_DIR "${CASS_SRC_DIR}/third_party/rapidjson")
  file(GLOB RAPIDJSON_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/*.h)
  file(GLOB RAPIDJSON_ERROR_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/error/*.h)
  file(GLOB RAPIDJSON_INTERNAL_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/internal/*.h)
  file(GLOB RAPIDJSON_MSIINTTYPES_HEADER_FILES ${RAPID_JSON_INCLUDE_DIR}/rapidjson/msiinttypes/*.h)
  source_group("Header Files\\rapidjson" FILES ${RAPIDJSON_HEADER_FILES})
  source_group("Header Files\\rapidjson\\error" FILES ${RAPIDJSON_ERROR_HEADER_FILES})
  source_group("Header Files\\rapidjson\\internal" FILES ${RAPIDJSON_INTERNAL_HEADER_FILES})
  source_group("Header Files\\rapidjson\\msiinttypes" FILES ${RAPIDJSON_MSIINTTYPES_HEADER_FILES})
  include_directories(${RAPID_JSON_INCLUDE_DIR})
endmacro()

#------------------------
# CassSimulacron
#
# Set up Simulacron for use in tests.
#
# Input: TESTS_SIMULACRON_SERVER_DIR
# Output: SIMULACRON_SERVER_JAR
#------------------------
macro(CassSimulacron)
  # Determine if Simulacron server can be executed (Java runtime is available)
  find_package(Java COMPONENTS Runtime)
  if(Java_Runtime_FOUND)
    # Determine the Simulacron jar file
    file(GLOB SIMULACRON_SERVER_JAR
      ${TESTS_SIMULACRON_SERVER_DIR}/simulacron-standalone-*.jar)
  endif()

  # Determine if the Simulacron server will be available for the tests
  if(NOT EXISTS ${SIMULACRON_SERVER_JAR})
    message(WARNING "Simulacron Not Found: Simulacron support will be disabled")
  endif()
endmacro()


#------------------------
# CassUseBoost
#
# Add includes, libraries, define flags required for using Boost.
#
# Input: CASS_USE_STATIC_LIBS, CASS_USE_BOOST_ATOMIC, CASS_INCLUDES, CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseBoost)

  # Allow for boost directory to be specified on the command line
  set(ENV{BOOST_ROOT} "${PROJECT_SOURCE_DIR}/lib/boost/")
  if(BOOST_ROOT_DIR)
    set(ENV{BOOST_ROOT} ${BOOST_ROOT_DIR})
  endif()

  # Ensure Boost auto linking is disabled (defaults to auto linking on Windows)
  if(WIN32)
    add_definitions(-DBOOST_ALL_NO_LIB)
  endif()

  # Determine if shared or static boost libraries should be used
  if(CASS_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS ON)
  else()
    set(Boost_USE_STATIC_LIBS OFF)
    add_definitions(-DBOOST_ALL_DYN_LINK)
  endif()
  set(Boost_USE_STATIC_RUNTIME OFF)
  set(Boost_USE_MULTITHREADED ON)
  add_definitions(-DBOOST_THREAD_USES_MOVE)

  # Ensure the driver components exist (optional)
  if(CASS_USE_BOOST_ATOMIC)
    find_package(Boost ${CASS_MINIMUM_BOOST_VERSION})
    if(NOT Boost_INCLUDE_DIR)
      message(FATAL_ERROR "Boost headers required to build driver because of -DCASS_USE_BOOST_ATOMIC=On")
    endif()

    # Assign Boost include and libraries
    set(CASS_INCLUDES ${CASS_INCLUDES} ${Boost_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${Boost_LIBRARIES})
  endif()

  # Determine if Boost components are available for test executables
  if(CASS_BUILD_INTEGRATION_TESTS)
    # Handle new required version of CMake for Boost v1.66.0 (Windows only)
    if(WIN32)
      find_package(Boost)
      if (Boost_FOUND)
        if(Boost_VERSION GREATER 106600 OR Boost_VERSION EQUAL 106600)
          # Ensure CMake version is v3.11.0+
          if (CMAKE_VERSION VERSION_LESS 3.11.0)
            message(FATAL_ERROR "Boost v${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION} requires CMake v3.11.0+."
              "Updgrade CMake or downgrade Boost to v${CASS_MINIMUM_BOOST_VERSION} - v1.65.1.")
          endif()
        endif()
      endif()
    endif()

    # Ensure Boost components are available
    find_package(Boost ${CASS_MINIMUM_BOOST_VERSION} COMPONENTS chrono system thread unit_test_framework)
    if(NOT Boost_FOUND)
      # Ensure Boost was not found due to minimum version requirement
      set(CASS_FOUND_BOOST_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
      if((CASS_FOUND_BOOST_VERSION VERSION_GREATER "${CASS_MINIMUM_BOOST_VERSION}")
        OR (CASS_FOUND_BOOST_VERSION VERSION_EQUAL "${CASS_MINIMUM_BOOST_VERSION}"))
        message(FATAL_ERROR "Boost [chrono, system, thread, and unit_test_framework] are required to build tests")
      else()
        message(FATAL_ERROR "Boost v${CASS_FOUND_BOOST_VERSION} Found: v${CASS_MINIMUM_BOOST_VERSION} or greater required")
      endif()
    endif()

    # Assign Boost include and libraries
    set(CASS_INCLUDES ${CASS_INCLUDES} ${Boost_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${Boost_LIBRARIES})
  endif()

  # Determine if additional Boost definitions are required for driver/executables
  if(NOT WIN32)
    # Handle explicit initialization warning in atomic/details/casts
    add_definitions(-Wno-missing-field-initializers)
  endif()
endmacro()

#------------------------
# CassUseOpenSSL
#
# Add includes and libraries required for using OpenSSL.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseOpenSSL)
  # Setup the paths and hints for OpenSSL
  set(_OPENSSL_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/openssl/")
  set(_OPENSSL_ROOT_HINTS ${OPENSSL_ROOT_DIR} $ENV{OPENSSL_ROOT_DIR})
  set(_OPENSSL_ROOT_HINTS_AND_PATHS
    HINTS ${_OPENSSL_ROOT_HINTS}
  PATHS ${_OPENSSL_ROOT_PATHS})

  # Discover OpenSSL and assign OpenSSL include and libraries
  find_package(OpenSSL REQUIRED)

  set(CASS_INCLUDES ${CASS_INCLUDES} ${OPENSSL_INCLUDE_DIR})
  set(CASS_LIBS ${CASS_LIBS} ${OPENSSL_LIBRARIES})
endmacro()

#------------------------
# CassUseTcmalloc
#
# Add libraries required for using tcmalloc.
#
# Input: CASS_LIBS
# Output: CASS_LIBS
#------------------------
macro(CassUseTcmalloc)
  # Setup the paths and hints for gperftools
  set(_GPERFTOOLS_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/gperftools/")
  set(_GPERFTOOLS_ROOT_HINTS ${GPERFTOOLS_ROOT_DIR} $ENV{GPERFTOOLS_ROOT_DIR})
  if(NOT WIN32)
    set(_GPERFTOOLS_ROOT_PATHS ${_GPERFTOOLS_ROOT_PATHS} "/usr/" "/usr/local/")
  endif()
  set(_GPERFTOOLS_ROOT_HINTS_AND_PATHS
    HINTS ${_GPERFTOOLS_ROOT_HINTS}
    PATHS ${_GPERFTOOLS_ROOT_PATHS})

  # Ensure gperftools (tcmalloc) was found
  find_library(GPERFLIBRARY_LIBRARY
    NAMES tcmalloc
    HINTS ${_GPERFTOOLS_LIBDIR} ${_GPERFTOOLS_ROOT_HINTS_AND_PATHS}
    PATH_SUFFIXES lib)
  find_package_handle_standard_args(Gperftools "Could NOT find gperftools, try to set the path to the gperftools root folder in the system variable GPERFTOOLS_ROOT_DIR"
    GPERFLIBRARY_LIBRARY)

  # Assign gperftools (tcmalloc) library
  set(CASS_LIBS ${CASS_LIBS} ${GPERFLIBRARY_LIBRARY})
endmacro()

#------------------------
# CassUseZlib
#
# Add includes and libraries required for using tcmalloc.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseZlib)
  # Setup the root directory for zlib
  set(ZLIB_ROOT "${PROJECT_SOURCE_DIR}/lib/zlib/")
  set(ZLIB_ROOT ${ZLIB_ROOT} ${ZLIB_ROOT_DIR} $ENV{ZLIB_ROOT_DIR})

  # Ensure zlib was found (assign zlib include/libraries or present warning)
  find_package(ZLIB)
  if(ZLIB_FOUND)
    # Determine if the static library needs to be used for Windows
    if(WIN32 AND CASS_USE_STATIC_LIBS)
      string(REPLACE "zlib.lib" "zlibstatic.lib" ZLIB_LIBRARIES "${ZLIB_LIBRARIES}")
    endif()

    # Assign zlib properties
    set(CASS_INCLUDES ${CASS_INCLUDES} ${ZLIB_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${ZLIB_LIBRARIES})
  else()
    message(WARNING "Could not find zlib, try to set the path to zlib root folder in the system variable ZLIB_ROOT_DIR")
    message(WARNING "zlib libraries will not be linked into build")
  endif()
endmacro()

#-------------------
# Compiler Flags
#-------------------

#------------------------
# CassSetCompilerFlags
#
# Detect compiler version and set compiler flags and defines
#
# Input: CASS_USE_STD_ATOMIC, CASS_USE_BOOST_ATOMIC, CASS_MULTICORE_COMPILATION
#        CASS_USE_STATIC_LIBS
# Output: CASS_USE_STD_ATOMIC, CASS_DRIVER_CXX_FLAGS, CASS_TEST_CXX_FLAGS,
#         CASS_EXAMPLE_C_FLAGS
#------------------------
macro(CassSetCompilerFlags)
  # Force OLD style of implicitly dereferencing variables
  if(POLICY CMP0054)
    cmake_policy(SET CMP0054 OLD)
  endif()

  # Determine if all GNU extensions should be enabled
  if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE")
  endif()

  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR
     "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # CMAKE_CXX_COMPILER variables do not exist in 2.6.4 (min version)
    # Parse the -dumpversion argument into the variable not already set
    if("${CMAKE_CXX_COMPILER_VERSION}" STREQUAL "")
      execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION)
    endif()

    # Enable debug "operator new" and operator delete" and strip symbols from shared library
    if(CASS_DEBUG_CUSTOM_ALLOC)
      if (APPLE)
        file(WRITE "${CMAKE_BINARY_DIR}/unexported.txt"
          "__Znwm\n"
          "__Znwm.eh\n"
          "__ZnwmRKSt9nothrow_t\n"
          "__ZnwmRKSt9nothrow_t.eh\n"
          "__ZdlPv\n"
          "__ZdlPv.eh\n"
          "__ZdlPvRKSt9nothrow_t\n"
          "__ZdlPvRKSt9nothrow_t.eh\n"
          "__Znam\n"
          "__Znam.eh\n"
          "__ZnamRKSt9nothrow_t\n"
          "__ZnamRKSt9nothrow_t.eh\n"
          "__ZdaPv\n"
          "__ZdaPv.eh\n"
          "__ZdaPvRKSt9nothrow_t\n"
          "__ZdaPvRKSt9nothrow_t.eh\n")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -unexported_symbols_list ${CMAKE_BINARY_DIR}/unexported.txt")
      elseif ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
        file(WRITE "${CMAKE_BINARY_DIR}/exported.ver"
          "DEBUG_CUSTOM_ALLOC\n"
          "{\n"
          "  global: *;\n"
          "  local:\n"
          "    _Znwm;\n"
          "    _Znwm.eh;\n"
          "    _ZnwmRKSt9nothrow_t;\n"
          "    _ZnwmRKSt9nothrow_t.eh;\n"
          "    _ZdlPv;\n"
          "    _ZdlPv.eh;\n"
          "    _ZdlPvRKSt9nothrow_t;\n"
          "    _ZdlPvRKSt9nothrow_t.eh;\n"
          "    _Znam;\n"
          "    _Znam.eh;\n"
          "    _ZnamRKSt9nothrow_t;\n"
          "    _ZnamRKSt9nothrow_t.eh;\n"
          "    _ZdaPv;\n"
          "    _ZdaPv.eh;\n"
          "    _ZdaPvRKSt9nothrow_t;\n"
          "    _ZdaPvRKSt9nothrow_t.eh;\n"
          "};\n")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--version-script=${CMAKE_BINARY_DIR}/exported.ver")
      endif()

      add_definitions(-DDEBUG_CUSTOM_ALLOCATOR)
    endif()
  endif()

  # Determine if std::atomic can be used for GCC, Clang, or MSVC
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
      # Version determined from: https://gcc.gnu.org/wiki/Atomic/GCCMM
      if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "4.7" OR
         CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "4.7")
        set(CASS_USE_STD_ATOMIC ON)
      endif()
    endif()
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # Version determined from: http://clang.llvm.org/cxx_status.html
    # 3.2 includes the full C++11 memory model, but 3.1 had atomic
    # support.
    if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "3.1" OR
       CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "3.1")
     set(CASS_USE_STD_ATOMIC ON)
    endif()
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    # Version determined from https://msdn.microsoft.com/en-us/library/hh874894
    # VS2012+/VS 11.0+/WindowsSDK v8.0+
    if(MSVC_VERSION GREATER 1700 OR
       MSVC_VERSION EQUAL 1700)
      set(CASS_USE_STD_ATOMIC ON)
    endif()
  endif()

  # Enable specific cass::Atomic implementation
  if(CASS_USE_BOOST_ATOMIC)
    message(STATUS "Using boost::atomic implementation for atomic operations")
    set(HAVE_BOOST_ATOMIC 1)
  elseif(CASS_USE_STD_ATOMIC)
    message(STATUS "Using std::atomic implementation for atomic operations")
    set(HAVE_STD_ATOMIC 1)
  endif()

  # Assign compiler specific flags
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    # Determine if multicore compilation should be enabled
    if(CASS_MULTICORE_COMPILATION)
      # Default multicore compilation with effective processors (see https://msdn.microsoft.com/en-us/library/bb385193.aspx)
      add_definitions("/MP")
    endif()

    # Remove/Replace linker flags in the event they are present
    string(REPLACE "/INCREMENTAL" "/INCREMENTAL:NO" CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG}")
    string(REPLACE "/INCREMENTAL" "/INCREMENTAL:NO" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

    # Create specific linker flags
    set(WINDOWS_LINKER_FLAGS "/INCREMENTAL:NO /LTCG /NODEFAULTLIB:LIBCMT.LIB /NODEFAULTLIB:LIBCMTD.LIB")
    if(CASS_USE_STATIC_LIBS)
      set(PROJECT_CXX_LINKER_FLAGS "${WINDOWS_LINKER_FLAGS}")
      set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${WINDOWS_LINKER_FLAGS}")
    endif()

    # On Visual C++ -pedantic flag is not used,
    # -fPIC is not used on Windows platform (all DLLs are
    # relocable), -Wall generates about 30k stupid warnings
    # that can hide useful ones.
    # Create specific warning disable compiler flags
    # TODO(mpenick): Fix these "possible loss of data" warnings
    add_definitions(/wd4244)
    add_definitions(/wd4267)
    add_definitions(/wd4800) # Performance warning due to automatic compiler casting from int to bool

    # Add preprocessor definitions for proper compilation
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)  # Remove warnings for not using safe functions (TODO: Fix codebase to be more secure for Visual Studio)
    add_definitions(-DNOMINMAX)                 # Does not define min/max macros

    # Create the project, example, and test flags
    set(CASS_DRIVER_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CASS_DRIVER_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CASS_EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
    # Enable bigobj for large object files during compilation (Cassandra types integration test)
    set(CASS_TEST_CXX_FLAGS "${CASS_DRIVER_CXX_FLAGS} ${WARNING_COMPILER_FLAGS} /bigobj")

    # Assign additional library requirements for Windows
    set(CASS_LIBS ${CASS_LIBS} iphlpapi psapi wsock32 crypt32 ws2_32 userenv)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # GCC specific compiler options
    # I disabled long-long warning because boost generates about 50 such warnings
    set(WARNING_COMPILER_FLAGS "-Wall -pedantic -Wextra -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros")

    if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "4.8" OR
       CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "4.8")
      set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-unused-local-typedefs")
    endif()

    # OpenSSL is deprecated on later versions of Mac OS X. The long-term solution
    # is to provide a CommonCryto implementation.
    if (APPLE AND CASS_USE_OPENSSL)
      set(CASS_DRIVER_CXX_FLAGS "${CASS_DRIVER_CXX_FLAGS} -Wno-deprecated-declarations")
      set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -Wno-deprecated-declarations")
    endif()

    # Enable C++11 support to use std::atomic
    if(CASS_USE_STD_ATOMIC)
      set(CASS_DRIVER_CXX_FLAGS "${CASS_DRIVER_CXX_FLAGS} -std=c++11")
      set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -std=c++11")
    endif()

    set(CASS_DRIVER_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CASS_DRIVER_CXX_FLAGS} ${WARNING_COMPILER_FLAGS} -Werror")
    set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CASS_EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # Clang/Intel specific compiler options
    # I disabled long-long warning because boost generates about 50 such warnings
    set(WARNING_COMPILER_FLAGS "-Wall -pedantic -Wextra -Wno-long-long -Wno-unused-parameter")
    set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-variadic-macros -Wno-zero-length-array")
    set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-unused-local-typedef -Wno-unknown-warning-option")

    # OpenSSL is deprecated on later versions of Mac OS X. The long-term solution
    # is to provide a CommonCryto implementation.
    if (APPLE AND CASS_USE_OPENSSL)
      set(CASS_DRIVER_CXX_FLAGS "${CASS_DRIVER_CXX_FLAGS} -Wno-deprecated-declarations")
      set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -Wno-deprecated-declarations")
    endif()

    # Enable C++11 support to use std::atomic
    if(CASS_USE_STD_ATOMIC)
      set(CASS_DRIVER_CXX_FLAGS "${CASS_DRIVER_CXX_FLAGS} -std=c++11")
      set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} -std=c++11")
    endif()

    set(CASS_DRIVER_CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CASS_DRIVER_CXX_FLAGS} ${WARNING_COMPILER_FLAGS} -Werror")
    set(CASS_TEST_CXX_FLAGS "${CASS_TEST_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CASS_EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} -std=c89 ${WARNING_COMPILER_FLAGS}")
  else()
    message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
  endif()
endmacro()

#-------------------
# Internal Includes and Source Files
#-------------------

#------------------------
# CassAddIncludes
#
# Add internal includes
#
# Input: CASS_INCLUDES
# Output: CASS_INCLUDES
#------------------------
macro(CassAddIncludes)
  set(CASS_INCLUDES
      ${CASS_ROOT_DIR}/include
      ${CASS_SRC_DIR}
      ${CASS_SRC_DIR}/ssl
      ${CASS_SRC_DIR}/third_party/rapidjson
      ${CASS_SRC_DIR}/third_party/rapidjson
      ${CASS_SRC_DIR}/third_party/sparsehash/src
      ${CASS_INCLUDES}
      )
  add_subdirectory(${CASS_SRC_DIR}/third_party/sparsehash)
endmacro()

#------------------------
# CassFindSourceFiles
#
# Gather the header and source files
#
# Input: CASS_ROOT_DIR, CASS_USE_BOOST_ATOMIC, CASS_USE_STD_ATOMIC
#        CASS_USE_OPENSSL
# Output: CASS_ALL_SOURCE_FILES
#------------------------
macro(CassFindSourceFiles)
  file(GLOB CASS_API_HEADER_FILES ${CASS_ROOT_DIR}/include/*.h)
  file(GLOB CASS_INC_FILES ${CASS_SRC_DIR}/*.hpp)
  file(GLOB CASS_SRC_FILES ${CASS_SRC_DIR}/*.cpp)

  if(${APPLE})
    list(REMOVE_ITEM CASS_SRC_FILES
      "${CASS_SRC_DIR}/get_time-unix.cpp"
      "${CASS_SRC_DIR}/get_time-win.cpp")
  elseif(${UNIX})
    list(REMOVE_ITEM CASS_SRC_FILES
      "${CASS_SRC_DIR}/get_time-mac.cpp"
      "${CASS_SRC_DIR}/get_time-win.cpp")
  elseif(${WIN32})
    list(REMOVE_ITEM CASS_SRC_FILES
      "${CASS_SRC_DIR}/get_time-mac.cpp"
      "${CASS_SRC_DIR}/get_time-unix.cpp")
  endif()

  # Determine atomic library to include
  if(CASS_USE_BOOST_ATOMIC)
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/atomic/atomic_boost.hpp)
  elseif(CASS_USE_STD_ATOMIC)
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/atomic/atomic_std.hpp)
  else()
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/atomic/atomic_intrinsics.hpp)
    if(WIN32)
      set(CASS_INC_FILES ${CASS_INC_FILES}
        ${CASS_SRC_DIR}/atomic/atomic_intrinsics_msvc.hpp)
    else()
      set(CASS_INC_FILES ${CASS_INC_FILES}
        ${CASS_SRC_DIR}/atomic/atomic_intrinsics_gcc.hpp)
    endif()
  endif()

  set(CASS_SRC_FILES ${CASS_SRC_FILES}
    ${CASS_SRC_DIR}/third_party/hdr_histogram/hdr_histogram.cpp)

  set(CASS_SRC_FILES ${CASS_SRC_FILES}
    ${CASS_SRC_DIR}/third_party/curl/hostcheck.cpp)

  # Determine if OpenSSL should be compiled in (or not)
  if(CASS_USE_OPENSSL)
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/ssl/ssl_openssl_impl.hpp
      ${CASS_SRC_DIR}/ssl/ring_buffer_bio.hpp)
    set(CASS_SRC_FILES ${CASS_SRC_FILES}
      ${CASS_SRC_DIR}/ssl/ssl_openssl_impl.cpp
      ${CASS_SRC_DIR}/ssl/ring_buffer_bio.cpp)
    set(HAVE_OPENSSL 1)
  else()
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/ssl/ssl_no_impl.hpp)
    set(CASS_SRC_FILES ${CASS_SRC_FILES}
      ${CASS_SRC_DIR}/ssl/ssl_no_impl.cpp)
  endif()

  set(CASS_ALL_SOURCE_FILES ${CASS_SRC_FILES} ${CASS_API_HEADER_FILES} ${CASS_INC_FILES})

  # Shorten the source file pathing for log messages
  foreach(SRC_FILE ${CASS_SRC_FILES})
    string(REPLACE "${CASS_ROOT_DIR}/" "" LOG_FILE_ ${SRC_FILE})
    set_source_files_properties(${SRC_FILE} PROPERTIES COMPILE_FLAGS -DLOG_FILE_=\\\"${LOG_FILE_}\\\")
  endforeach()
endmacro()

#------------------------
# CassConfigure
#
# Generate cassconfig.hpp from cassconfig.hpp.in
#
# Input: CASS_ROOT_DIR, CASS_SRC_DIR
#------------------------
macro(CassConfigure)
  # Determine random availability
  if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    check_symbol_exists(GRND_NONBLOCK "linux/random.h" HAVE_GETRANDOM)
  else()
    check_symbol_exists(arc4random_buf "stdlib.h" HAVE_ARC4RANDOM)
  endif()
  # Determine if sigpipe is available
  check_symbol_exists(SO_NOSIGPIPE "sys/socket.h;sys/types.h" HAVE_NOSIGPIPE)
  check_symbol_exists(sigtimedwait "signal.h" HAVE_SIGTIMEDWAIT)
  if (NOT WIN32 AND NOT HAVE_NOSIGPIPE AND NOT HAVE_SIGTIMEDWAIT)
    message(WARNING "Unable to handle SIGPIPE on your platform")
  endif()

  # Determine if hash is in the tr1 namespace
  string(REPLACE "::" ";" HASH_NAMESPACE_LIST ${HASH_NAMESPACE})
  foreach(NAMESPACE ${HASH_NAMESPACE_LIST})
    if(NAMESPACE STREQUAL "tr1")
      set(HASH_IN_TR1 1)
    endif()
  endforeach()

  # Check for GCC compiler builtins
  if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    check_cxx_source_compiles("int main() { return __builtin_bswap32(42); }" HAVE_BUILTIN_BSWAP32)
    check_cxx_source_compiles("int main() { return __builtin_bswap64(42); }" HAVE_BUILTIN_BSWAP64)
  endif()

  # Generate the cassconfig.hpp file
  configure_file(${CASS_ROOT_DIR}/cassconfig.hpp.in ${CASS_SRC_DIR}/cassconfig.hpp)
endmacro()
