cmake_minimum_required(VERSION 2.6.4)

#------------------------
# GtestFramework
#
# Initialize the Google Test framework
#
# Input: VENDOR_DIR
# Output: GOOGLE_TEST_DIR, GOOGLE_TEST_HEADER_FILES, GOOGLE_TEST_SOURCE_FILES
#------------------------
macro(GtestFramework)
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
  endif()
endmacro()

#------------------------
# GtestOptions
#
# Initialize CMake options for projects that use Google Test
#
# Output: INTEGRATION_VERBOSE_LOGGING, CCM_VERBOSE_LOGGING, USE_VISUAL_LEAK_DETECTOR
#------------------------
macro(GtestOptions)
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
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    if(MSVC_VERSION GREATER 1800 OR MSVC_VERSION EQUAL 1800) # VS2013+/VS 12.0+
      add_definitions(-DGTEST_LANG_CXX11=1)
      add_definitions(-DGTEST_HAS_TR1_TUPLE=0)
    endif()
  endif()
endmacro()

#------------------------
# GtestSourceGroups
#
# Configure source groups for various core driver files.
#
# Input: CASS_SRC_DIR, CASS_API_HEADER_FILES, LIBUV_INCLUDE_FILES
# Output: CPP_DRIVER_HEADER_SOURCE_FILES, CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES,
#         CPP_DRIVER_SOURCE_FILES
#------------------------
macro(GtestSourceGroups)
  # Add utility functionality from the Cassandra driver
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
endmacro()

#------------------------
# GtestIntegrationTestFiles
#
# Gather the names of all integration test files into various output
# variables, scoped by the project (e.g. CASS_* vs. DSE_* variables).
#
# Arguments:
#   integration_tests_source_dir - directory containing integration test
#      sources. Note, this is the 'src' directory, not the 'src/tests'
#      directory.
#   prefix - CASS or DSE, used to prefix the variables being set.
#------------------------
macro(GtestIntegrationTestFiles integration_tests_source_dir prefix)
  file(GLOB ${prefix}_INTEGRATION_TESTS_INCLUDE_FILES ${integration_tests_source_dir}/*.hpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_CCM_INCLUDE_FILES ${integration_tests_source_dir}/ccm/*.hpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES ${integration_tests_source_dir}/objects/*.hpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_POLICIES_INCLUDE_FILES ${integration_tests_source_dir}/policies/*.hpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_VALUES_INCLUDE_FILES ${integration_tests_source_dir}/values/*.hpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_SOURCE_FILES ${integration_tests_source_dir}/*.cpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_CCM_SOURCE_FILES ${integration_tests_source_dir}/ccm/*.cpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_OBJECTS_SOURCE_FILES ${integration_tests_source_dir}/objects/*.cpp)
  file(GLOB ${prefix}_INTEGRATION_TESTS_TESTS_SOURCE_FILES ${integration_tests_source_dir}/tests/*.cpp)
endmacro()

#------------------------
# GtestIntegrationTestSourceGroups
#
# Configure source groups for various integration test files.
#
# Input: INTEGRATION_TESTS_INCLUDE_FILES, INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES,
#        INTEGRATION_TESTS_VALUES_INCLUDE_FILES, INTEGRATION_TESTS_SOURCE_FILES,
#        INTEGRATION_TESTS_OBJECTS_SOURCE_FILES, INTEGRATION_TESTS_TESTS_SOURCE_FILES
#------------------------
macro(GtestIntegrationTestSourceGroups)
  source_group("Header Files" FILES ${INTEGRATION_TESTS_INCLUDE_FILES})
  source_group("Header Files\\ccm" FILES ${INTEGRATION_TESTS_CCM_INCLUDE_FILES})
  source_group("Header Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES})
  source_group("Header Files\\policies" FILES ${INTEGRATION_TESTS_POLICIES_INCLUDE_FILES})
  source_group("Header Files\\values" FILES ${INTEGRATION_TESTS_VALUES_INCLUDE_FILES})
  source_group("Source Files" FILES ${INTEGRATION_TESTS_SOURCE_FILES})
  source_group("Source Files\\ccm" FILES ${INTEGRATION_TESTS_CCM_SOURCE_FILES})
  source_group("Source Files\\objects" FILES ${INTEGRATION_TESTS_OBJECTS_SOURCE_FILES})
  source_group("Source Files\\tests" FILES ${INTEGRATION_TESTS_TESTS_SOURCE_FILES})
endmacro()

#------------------------
# GtestCommonIntegrationTestSourceFiles
#
# Gather the names of all shared files used in building the test executable for
# the project.
#
# Input: many
# Output: COMMON_INTEGRATION_TEST_INCLUDE_FILES, COMMON_INTEGRATION_TEST_SOURCE_FILES
#------------------------
macro(GtestCommonIntegrationTestSourceFiles)
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
endmacro()

#------------------------
# GtestLibssh2IntegrationTest
#
# Determine if libssh2 has been requested.
#
# Input: CASS_USE_LIBSSH2
# Output: INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES, INTEGRATION_TESTS_LIBSSH2_LIBRARIES
#------------------------
macro(GtestLibssh2IntegrationTest)
  if(CASS_USE_LIBSSH2)
  # Allow for libssh2 directory to be specified on the command line
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
    find_package(Libssh2)
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
    include_directories(${LIBSSH2_INCLUDE_DIRS})
    set(INTEGRATION_TESTS_LIBSSH2_LIBRARIES ${LIBSSH2_LIBRARIES})
    # Build up the includes and libraries for CCM dependencies
    add_definitions(-DCASS_USE_LIBSSH2 -DOPENSSL_CLEANUP)
    file(GLOB INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES ${LIBSSH2_INCLUDE_DIRS}/*.h)
    source_group("Header Files\\libssh2" FILES ${INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES})

    if(WIN32)
      add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
      set(INTEGRATION_TESTS_LIBSSH2_LIBRARIES ${INTEGRATION_TESTS_LIBSSH2_LIBRARIES} wsock32 ws2_32)
    endif()
  endif()
endmacro()

#------------------------
# GtestUnitTests
#
# Configure unit tests to be built.
#
# Arguments:
#   extra_files         - List of extra files to build into the test executable, if
#                         any.
#   extra_includes      - List of extra includes to include into the text
#                         executable, if any.
#   excluded_test_files - List of test files to exclude from the build.
#------------------------
macro(GtestUnitTests extra_files extra_includes excluded_test_files)
  set(UNIT_TESTS_NAME "cassandra-unit-tests")
  set(UNIT_TESTS_DISPLAY_NAME "Unit Tests")
  set(UNIT_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/unit")

  # The unit tests use `test::Utils::msleep()` and this is the minimum include
  # and source files required to shared that code.
  set(INTEGRATION_TESTS_SOURCE_DIR ${CASS_ROOT_DIR}/tests/src/integration)
  set(INTEGRATION_TESTS_CCM_SOURCE_DIR ${INTEGRATION_TESTS_SOURCE_DIR}/ccm)
  set(INTEGRATION_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/test_utils.cpp)
  set(INTEGRATION_TESTS_CCM_SOURCE_FILES "${INTEGRATION_TESTS_CCM_SOURCE_DIR}/tsocket.cpp")
  file(GLOB UNIT_TESTS_INCLUDE_FILES ${UNIT_TESTS_SOURCE_DIR}/*.hpp )
  file(GLOB UNIT_TESTS_SOURCE_FILES ${UNIT_TESTS_SOURCE_DIR}/*.cpp)
  file(GLOB UNIT_TESTS_TESTS_SOURCE_FILES ${UNIT_TESTS_SOURCE_DIR}/tests/*.cpp)
  foreach(excluded_test_file ${excluded_test_files})
    list(REMOVE_ITEM
      UNIT_TESTS_TESTS_SOURCE_FILES
      "${UNIT_TESTS_SOURCE_DIR}/tests/${excluded_test_file}")
  endforeach()
  source_group("Header Files" FILES ${UNIT_TESTS_INCLUDE_FILES})
  source_group("Source Files" FILES ${UNIT_TESTS_SOURCE_FILES})
  source_group("Source Files\\tests" FILES ${UNIT_TESTS_TESTS_SOURCE_FILES})
  add_executable(${UNIT_TESTS_NAME}
                 ${extra_files}
                 ${UNIT_TESTS_SOURCE_FILES}
                 ${UNIT_TESTS_TESTS_SOURCE_FILES}
                 ${INTEGRATION_TESTS_SOURCE_FILES}
                 ${INTEGRATION_TESTS_CCM_SOURCE_FILES}
                 ${CPP_DRIVER_SOURCE_FILES}
                 ${UNIT_TESTS_INCLUDE_FILES}
                 ${CASS_API_HEADER_FILES}
                 ${CPP_DRIVER_INCLUDE_FILES}
                 ${CPP_DRIVER_HEADER_SOURCE_FILES}
                 ${CPP_DRIVER_HEADER_SOURCE_ATOMIC_FILES}
                 ${GOOGLE_TEST_HEADER_FILES}
                 ${GOOGLE_TEST_SOURCE_FILES})
  if(CMAKE_VERSION VERSION_LESS "2.8.11")
    include_directories(${extra_includes}
                        ${UNIT_TESTS_SOURCE_DIR}
                        ${INTEGRATION_TESTS_SOURCE_DIR}
                        ${INTEGRATION_TESTS_CCM_SOURCE_DIR})
  else()
    target_include_directories(${UNIT_TESTS_NAME}
                               PUBLIC ${extra_includes}
                                      ${UNIT_TESTS_SOURCE_DIR}
                                      ${INTEGRATION_TESTS_SOURCE_DIR}
                                      ${INTEGRATION_TESTS_CCM_SOURCE_DIR})
  endif()
  target_link_libraries(${UNIT_TESTS_NAME}
                        ${DSE_LIBS}
                        ${PROJECT_LIB_NAME_TARGET})
  set_target_properties(${UNIT_TESTS_NAME} PROPERTIES PROJECT_LABEL "${UNIT_TESTS_DISPLAY_NAME}"
                                                      FOLDER "Tests"
                                                      COMPILE_FLAGS "${TEST_CMAKE_CXX_FLAGS}")

  # Add the unit tests to be executed by ctest (see CMake BUILD_TESTING)
  add_test(${UNIT_TESTS_DISPLAY_NAME} ${UNIT_TESTS_NAME}
      COMMAND ${UNIT_TESTS_NAME})
  set_tests_properties(${UNIT_TESTS_DISPLAY_NAME} PROPERTIES TIMEOUT 5)
endmacro()
