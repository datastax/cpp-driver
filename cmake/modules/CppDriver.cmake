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
  # Retrieve version from header file (include string suffix)
  file(STRINGS ${version_header_file} _VERSION_PARTS
      REGEX "^#define[ \t]+${prefix}_VERSION_(MAJOR|MINOR|PATCH|SUFFIX)[ \t]+([0-9]+|\"([^\"]+)\")$")

  foreach(part MAJOR MINOR PATCH SUFFIX)
    string(REGEX MATCH "${prefix}_VERSION_${part}[ \t]+([0-9]+|\"([^\"]+)\")"
           ${project_name}_VERSION_${part} ${_VERSION_PARTS})
    # Extract version numbers
    if (${project_name}_VERSION_${part})
      string(REGEX REPLACE "${prefix}_VERSION_${part}[ \t]+([0-9]+|\"([^\"]+)\")" "\\1"
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
  if(NOT ${project_name}_VERSION_SUFFIX STREQUAL "")
    string(REPLACE "\"" ""
        ${project_name}_VERSION_SUFFIX ${${project_name}_VERSION_SUFFIX})
    set(${project_name}_VERSION_STRING
      "${${project_name}_VERSION_STRING}-${${project_name}_VERSION_SUFFIX}")
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
# Configure enabled optional dependencies if found or if Windows use an
# external project to build OpenSSL and zlib.
#
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassOptionalDependencies)
  # Boost
  if(CASS_USE_BOOST_ATOMIC)
    CassUseBoost()
  endif()

  # Kerberos
  if(CASS_USE_KERBEROS)
    CassUseKerberos()
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
# Input: PROJECT_LIB_NAME, PROJECT_VERSION_STRING, PROJECT_VERSION_MAJOR
#------------------------
macro(CassConfigureShared prefix)
  target_link_libraries(${PROJECT_LIB_NAME} ${${prefix}_LIBS})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES
      COMPILE_PDB_NAME "${PROJECT_LIB_NAME}"
      COMPILE_PDB_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  set(SHARED_COMPILE_FLAGS "-D${prefix}_BUILDING")
  if("${prefix}" STREQUAL "DSE")
    set(SHARED_COMPILE_FLAGS "${SHARED_COMPILE_FLAGS} -DCASS_BUILDING")
  endif()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set_property(
        TARGET ${PROJECT_LIB_NAME}
        APPEND PROPERTY COMPILE_FLAGS "${SHARED_COMPILE_FLAGS} -Wconversion -Wno-sign-conversion -Wno-shorten-64-to-32 -Wno-undefined-var-template -Werror")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU") # To many superfluous warnings generated with GCC when using -Wconversion (see: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=40752)
    set_property(
        TARGET ${PROJECT_LIB_NAME}
        APPEND PROPERTY COMPILE_FLAGS "${SHARED_COMPILE_FLAGS} -Werror")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set_property(
        TARGET ${PROJECT_LIB_NAME}
        APPEND PROPERTY COMPILE_FLAGS "${SHARED_COMPILE_FLAGS} /we4800")
  endif()
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
#        *_USE_STATIC_LIBS
#------------------------
macro(CassConfigureStatic prefix)
  target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${${prefix}_LIBS})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES
      COMPILE_PDB_NAME "${PROJECT_LIB_NAME_STATIC}"
      COMPILE_PDB_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  set(STATIC_COMPILE_FLAGS "-D${prefix}_STATIC")
  if("${prefix}" STREQUAL "DSE")
    set(STATIC_COMPILE_FLAGS "${STATIC_COMPILE_FLAGS} -DCASS_STATIC")
  endif()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set_property(
        TARGET ${PROJECT_LIB_NAME_STATIC}
        APPEND PROPERTY COMPILE_FLAGS "${STATIC_COMPILE_FLAGS} -Wconversion -Wno-sign-conversion -Wno-shorten-64-to-32 -Wno-undefined-var-template -Werror")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU") # To many superfluous warnings generated with GCC when using -Wconversion (see: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=40752)
    set_property(
        TARGET ${PROJECT_LIB_NAME_STATIC}
        APPEND PROPERTY COMPILE_FLAGS "${STATIC_COMPILE_FLAGS} -Werror")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set_property(
        TARGET ${PROJECT_LIB_NAME_STATIC}
        APPEND PROPERTY COMPILE_FLAGS "${STATIC_COMPILE_FLAGS} /we4800")
  endif()

  # Update the CXX flags to indicate the use of the static library
  if(${prefix}_USE_STATIC_LIBS)
    set(TEST_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STATIC_COMPILE_FLAGS}") # Unit and integration test executables
    set(EXAMPLE_CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${STATIC_COMPILE_FLAGS}") # Example executables
  endif()
endmacro()

#------------------------
# CassConfigureExamples
#
# Configure the project to build examples.
#
# Arguments:
#    examples_dir - directory containing all example sub-directories.
macro(CassBuildExamples examples_dir)
  file(GLOB_RECURSE EXAMPLES_TO_BUILD "${examples_dir}/*/CMakeLists.txt")
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

  # Determine the header install dir
  if (CASS_INSTALL_HEADER_IN_SUBDIR)
    if (CASS_INSTALL_HEADER_SUBDIR_NAME)
      # User-specified include sub-dir
      set(INSTALL_HEADER_DIR "include/${CASS_INSTALL_HEADER_SUBDIR_NAME}")
    else()
      # Default subdir location is 'include/cassandra'
      set(INSTALL_HEADER_DIR "include/${PROJECT_NAME_STRING}")
    endif()
  else()
    # Default header install location is 'include'
    set(INSTALL_HEADER_DIR "include")
  endif()

  if(${var_prefix}_INSTALL_PKG_CONFIG)
    if(NOT WIN32)
      find_package(PkgConfig)
      if(PKG_CONFIG_FOUND)
        set(prefix ${CMAKE_INSTALL_PREFIX})
        set(exec_prefix ${CMAKE_INSTALL_PREFIX})
        set(libdir ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR})
        set(includedir ${CMAKE_INSTALL_PREFIX}/${INSTALL_HEADER_DIR})
        set(version ${PROJECT_VERSION_STRING})
      endif()
    endif()
  endif()

  # Determine if the header should be installed
  if(${var_prefix}_INSTALL_HEADER)
    install(FILES ${${var_prefix}_API_HEADER_FILES} DESTINATION ${INSTALL_HEADER_DIR})
  endif()

  # Install the dynamic/shared library
  if(${var_prefix}_BUILD_SHARED)
    install(TARGETS ${PROJECT_LIB_NAME}
        RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable/pdb files
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
    if(WIN32)
      install(FILES $<TARGET_PDB_FILE:${PROJECT_LIB_NAME}>
        DESTINATION "${INSTALL_DLL_EXE_DIR}"
        OPTIONAL)
    endif()
  endif()

  if(${var_prefix}_BUILD_STATIC)
    install(TARGETS ${PROJECT_LIB_NAME_STATIC}
        RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable/pdb files
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
  if (CASS_BUILD_INTEGRATION_TESTS OR CASS_BUILD_UNIT_TESTS)
    add_subdirectory(${CASS_ROOT_DIR}/tests)
  endif()
endmacro()

#---------------
# Dependencies
#---------------

#------------------------
# CassUseLibuv
#
# Add includes and libraries required for using libuv if found or if Windows
# use an external project to build libuv.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseLibuv)
  # Setup the paths and hints for libuv
  if(NOT LIBUV_ROOT_DIR)
    if(EXISTS "${PROJECT_SOURCE_DIR}/lib/libuv/")
      set(LIBUV_ROOT_DIR "${PROJECT_SOURCE_DIR}/lib/libuv/")
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/build/libs/libuv/")
      set(LIBUV_ROOT_DIR "${PROJECT_SOURCE_DIR}/build/libs/libuv/")
    endif()
  endif()

  # Ensure libuv was found
  find_package(Libuv "1.0.0" QUIET)
  if(WIN32 AND NOT LIBUV_FOUND)
    message(STATUS "Unable to Locate libuv: Third party build step will be performed")
    include(ExternalProject-libuv)
  elseif(NOT LIBUV_FOUND)
    message(FATAL_ERROR "Unable to Locate libuv: libuv v1.0.0+ is required")
  endif()

  if(LIBUV_VERSION VERSION_LESS "1.0")
    message(FATAL_ERROR "Libuv version ${LIBUV_VERSION} is not "
      " supported. Please updgrade to libuv version 1.0 or greater in order to "
      "utilize the driver.")
  endif()

  if(LIBUV_VERSION VERSION_LESS "1.6")
    message(WARNING "Libuv version ${LIBUV_VERSION} does not support custom "
    "memory allocators (version 1.6 or greater required)")
  endif()

  # Assign libuv include and libraries
  set(CASS_INCLUDES ${CASS_INCLUDES} ${LIBUV_INCLUDE_DIRS})
  set(CASS_LIBS ${CASS_LIBS} ${LIBUV_LIBRARIES})

  # libuv and gtests require thread library
  set(CMAKE_THREAD_PREFER_PTHREAD 1)
  set(THREADS_PREFER_PTHREAD_FLAG 1)
  find_package(Threads REQUIRED)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_THREAD_LIBS_INIT}")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_THREAD_LIBS_INIT}")
  if(NOT WIN32 AND ${CMAKE_VERSION} VERSION_LESS "3.1.0")
    # FindThreads in CMake versions < v3.1.0 do not have the THREADS_PREFER_PTHREAD_FLAG to prefer -pthread
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
  endif()
endmacro()

#------------------------
# Optional Dependencies
#------------------------

# Minimum supported version of Boost
set(CASS_MINIMUM_BOOST_VERSION 1.59.0)

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
# CassMiniZip
#
# Set some MINIZIP_* variables, set up some source_group's, and add the
# MINIZIP include dir to our list of include dirs.
#
# Input: CASS_SRC_DIR
# Output: MINIZIP_INCLUDE_DIR, MINIZIP_HEADER_FILES, MINIZIP_SOURCE_FILES
#------------------------
macro(CassMiniZip)
  if (ZLIB_FOUND)
    set(MINIZIP_INCLUDE_DIR "${CASS_SRC_DIR}/third_party/minizip")
    set(MINIZIP_HEADER_FILES ${MINIZIP_INCLUDE_DIR}/crypt.h
                             ${MINIZIP_INCLUDE_DIR}/ioapi.h
                             ${MINIZIP_INCLUDE_DIR}/unzip.h)
    set(MINIZIP_SOURCE_FILES ${MINIZIP_INCLUDE_DIR}/ioapi.c
                             ${MINIZIP_INCLUDE_DIR}/unzip.c)
    source_group("Header Files\\minizip" FILES ${MINIZIP_HEADER_FILES})
    source_group("Source Files\\minizip" FILES ${MINIZIP_SOURCE_FILES})
    include_directories(${MINIZIP_INCLUDE_DIR})
  endif()
endmacro()

#------------------------
# CassHttpParser
#
# Set some HTTP_PARSER_* variables, set up some source_group's, and add the
# HTTP_PARSER include dir to our list of include dirs.
#
# Input: CASS_SRC_DIR
# Output: HTTP_PARSER_INCLUDE_DIR, HTTP_PARSER_HEADER_FILES,
#         HTTP_PARSER_SOURCE_FILES
#------------------------
macro(CassHttpParser)
  set(HTTP_PARSER_INCLUDE_DIR "${CASS_SRC_DIR}/third_party/http-parser")
  set(HTTP_PARSER_HEADER_FILES ${HTTP_PARSER_INCLUDE_DIR}/http_parser.h)
  set(HTTP_PARSER_SOURCE_FILES ${HTTP_PARSER_INCLUDE_DIR}/http_parser.c)
  source_group("Header Files\\http-parser" FILES ${HTTP_PARSER_HEADER_FILES})
  source_group("Source Files\\http-parser" FILES ${HTTP_PARSER_SOURCE_FILES})
  include_directories(${HTTP_PARSER_INCLUDE_DIR})
endmacro()

#------------------------
# CassUseBoost
#
# Add includes and define flags required for using Boost if found.
#
# Input: CASS_USE_BOOST_ATOMIC, CASS_INCLUDES
# Output: CASS_INCLUDES
#------------------------
macro(CassUseBoost)
  # Allow for boost directory to be specified on the command line
  if(NOT DEFINED ENV{BOOST_ROOT})
    if(EXISTS "${PROJECT_SOURCE_DIR}/lib/boost/")
      set(ENV{BOOST_ROOT} "${PROJECT_SOURCE_DIR}/lib/boost/")
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/build/libs/boost/")
      set(ENV{BOOST_ROOT} "${PROJECT_SOURCE_DIR}/build/libs/boost/")
    endif()
  endif()
  if(BOOST_ROOT_DIR)
    if(EXISTS ${BOOST_ROOT_DIR})
      set(ENV{BOOST_ROOT} ${BOOST_ROOT_DIR})
    endif()
  endif()

  # Ensure Boost auto linking is disabled (defaults to auto linking on Windows)
  if(WIN32)
    add_definitions(-DBOOST_ALL_NO_LIB)
  endif()

  # Check for general Boost availability
  find_package(Boost ${CASS_MINIMUM_BOOST_VERSION} QUIET)
  if(CASS_USE_BOOST_ATOMIC)
    if(NOT Boost_INCLUDE_DIRS)
      message(FATAL_ERROR "Boost headers required to build driver because of -DCASS_USE_BOOST_ATOMIC=On")
    endif()

    # Assign Boost include for atomics
    set(CASS_INCLUDES ${CASS_INCLUDES} ${Boost_INCLUDE_DIRS})
  endif()

  # Determine if additional Boost definitions are required for driver/executables
  if(NOT WIN32)
    # Handle explicit initialization warning in atomic/details/casts
    add_definitions(-Wno-missing-field-initializers)
  endif()
endmacro()

#------------------------
# CassUseKerberos
#
# Add includes and libraries required for using Kerberos if found.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseKerberos)
  # Discover Kerberos and assign Kerberos include and libraries
  find_package(Kerberos REQUIRED)
  set(CASS_INCLUDES ${CASS_INCLUDES} ${KERBEROS_INCLUDE_DIR})
  set(CASS_LIBS ${CASS_LIBS} ${KERBEROS_LIBRARIES})
endmacro()

#------------------------
# CassUseOpenSSL
#
# Add includes and libraries required for using OpenSSL if found or if Windows
# use an external project to build OpenSSL.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseOpenSSL)
  if(NOT WIN32)
    set(_OPENSSL_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/openssl/")
    set(_OPENSSL_ROOT_HINTS ${OPENSSL_ROOT_DIR} $ENV{OPENSSL_ROOT_DIR})
    set(_OPENSSL_ROOT_HINTS_AND_PATHS
        HINTS ${_OPENSSL_ROOT_HINTS}
        PATHS ${_OPENSSL_ROOT_PATHS})
  else()
    if(NOT DEFINED OPENSSL_ROOT_DIR)
      # FindOpenSSL overrides _OPENSSL_ROOT_HINTS and _OPENSSL_ROOT_PATHS on Windows
      # however it utilizes OPENSSL_ROOT_DIR when it sets these values
      set(OPENSSL_ROOT_DIR "${PROJECT_SOURCE_DIR}/lib/openssl/"
                           "${PROJECT_SOURCE_DIR}/build/libs/openssl/")
    endif()
  endif()

  # Discover OpenSSL and assign OpenSSL include and libraries
  if(WIN32 AND OPENSSL_VERSION) # Store the current version of OpenSSL to prevent corruption
    set(SAVED_OPENSSL_VERSION ${OPENSSL_VERSION})
  endif()
  find_package(OpenSSL)
  if(WIN32 AND NOT OPENSSL_FOUND)
    message(STATUS "Unable to Locate OpenSSL: Third party build step will be performed")
    if(SAVED_OPENSSL_VERSION)
      set(OPENSSL_VERSION ${SAVED_OPENSSL_VERSION})
    endif()
    include(ExternalProject-OpenSSL)
  elseif(NOT OPENSSL_FOUND)
    message(FATAL_ERROR "Unable to Locate OpenSSL: Ensure OpenSSL is installed in order to build the driver")
  else()
    set(openssl_name "OpenSSL")
    if(LIBRESSL_FOUND)
      set(openssl_name "LibreSSL")
    endif()
    message(STATUS "${openssl_name} version: v${OPENSSL_VERSION}")
  endif()

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
# Add includes and libraries required for using zlib if found or if Windows use
# an external project to build zlib.
#
# Input: CASS_INCLUDES and CASS_LIBS
# Output: CASS_INCLUDES and CASS_LIBS
#------------------------
macro(CassUseZlib)
  if(NOT ZLIB_LIBRARY_NAME)
    # Setup the root directory for zlib
    set(ZLIB_ROOT "${PROJECT_SOURCE_DIR}/lib/zlib/"
                  "${PROJECT_SOURCE_DIR}/build/libs/zlib/")
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
      set(HAVE_ZLIB On)
    else()
      message(WARNING "Could not find zlib, try to set the path to zlib root folder in the system variable ZLIB_ROOT_DIR")
      message(WARNING "zlib libraries will not be linked into build")
    endif()
  else()
    # Assign zlib properties
    set(CASS_INCLUDES ${CASS_INCLUDES} ${ZLIB_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${ZLIB_LIBRARIES})
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

    # On Visual C++ -pedantic flag is not used,
    # -fPIC is not used on Windows platform (all DLLs are
    # relocable), -Wall generates about 30k stupid warnings
    # that can hide useful ones.
    # Create specific warning disable compiler flags
    # TODO(mpenick): Fix these "possible loss of data" warnings
    add_definitions(/wd4244)
    add_definitions(/wd4267)

    # Add preprocessor definitions for proper compilation
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)  # Remove warnings for not using safe functions (TODO: Fix codebase to be more secure for Visual Studio)
    add_definitions(-DNOMINMAX)                 # Does not define min/max macros
    add_definitions(-D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING) # Remove warnings for TR1 deprecation (Visual Studio 15 2017); caused by sparsehash

    # Create the project, example, and test flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")

    # Assign additional library requirements for Windows
    set(CASS_LIBS ${CASS_LIBS} iphlpapi psapi wsock32 crypt32 ws2_32 userenv version)
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
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
    endif()

    # Enable C++11 support to use std::atomic
    if(CASS_USE_STD_ATOMIC)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # Clang/Intel specific compiler options
    # I disabled long-long warning because boost generates about 50 such warnings
    set(WARNING_COMPILER_FLAGS "-Wall -pedantic -Wextra -Wno-long-long -Wno-unused-parameter")
    set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-variadic-macros -Wno-zero-length-array")
    set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-unused-local-typedef -Wno-unknown-warning-option")

    # OpenSSL is deprecated on later versions of Mac OS X. The long-term solution
    # is to provide a CommonCryto implementation.
    if (APPLE AND CASS_USE_OPENSSL)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
    endif()

    # Enable C++11 support to use std::atomic
    if(CASS_USE_STD_ATOMIC)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
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

  # Determine if Kerberos should be compiled in (or not)
  if(CASS_USE_KERBEROS)
    set(CASS_INC_FILES ${CASS_INC_FILES}
      ${CASS_SRC_DIR}/gssapi/dse_auth_gssapi.hpp)
    set(CASS_SRC_FILES ${CASS_SRC_FILES}
      ${CASS_SRC_DIR}/gssapi/dse_auth_gssapi.cpp)
    set(HAVE_KERBEROS 1)
  endif()

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

  CassMiniZip()
  set(CASS_INC_FILES ${CASS_INC_FILES} ${MINIZIP_HEADER_FILES})
  set(CASS_SRC_FILES ${CASS_SRC_FILES} ${MINIZIP_SOURCE_FILES})

  CassHttpParser()
  set(CASS_INC_FILES ${CASS_INC_FILES} ${HTTP_PARSER_HEADER_FILES})
  set(CASS_SRC_FILES ${CASS_SRC_FILES} ${HTTP_PARSER_SOURCE_FILES})

  set(CASS_ALL_SOURCE_FILES ${CASS_SRC_FILES} ${CASS_API_HEADER_FILES} ${CASS_INC_FILES})
endmacro()

#------------------------
# CassConfigure
#
# Generate driver_config.hpp from driver_config.hpp.in
#
# Input: CASS_ROOT_DIR, CASS_SRC_DIR
#------------------------
macro(CassConfigure)
  # Determine random availability
  if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    check_symbol_exists(GRND_NONBLOCK "linux/random.h" HAVE_GETRANDOM)
    if(CASS_USE_TIMERFD)
      check_symbol_exists(timerfd_create "sys/timerfd.h" HAVE_TIMERFD)
    endif()
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
  if(NOT "${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    check_cxx_source_compiles("int main() { return __builtin_bswap32(42); }" HAVE_BUILTIN_BSWAP32)
    check_cxx_source_compiles("int main() { return __builtin_bswap64(42); }" HAVE_BUILTIN_BSWAP64)
  endif()

  # Generate the driver_config.hpp file
  configure_file(${CASS_ROOT_DIR}/driver_config.hpp.in ${CASS_SRC_DIR}/driver_config.hpp)
endmacro()
