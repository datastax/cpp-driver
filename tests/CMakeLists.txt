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

include(Gtest)

GtestOptions()

# Build the base include paths for the tests
set(TESTS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(VENDOR_DIR "${TESTS_SOURCE_DIR}/vendor")
include_directories(${VENDOR_DIR})

#------------------------------
# Google test framework
#------------------------------
GtestFramework()

#------------------------------
# Integration test executable
#------------------------------
if(CASS_BUILD_INTEGRATION_TESTS)
  CassRapidJson()

  set(INTEGRATION_TESTS_NAME "cassandra-integration-tests")
  set(INTEGRATION_TESTS_DISPLAY_NAME "Integration Tests")
  set(INTEGRATION_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration")

  GtestIntegrationTestFiles(${INTEGRATION_TESTS_SOURCE_DIR} "CASS")

  # Promote the CASS_* variables to be non-prefixed "final" variables.
  foreach(var_name INTEGRATION_TESTS_INCLUDE_FILES INTEGRATION_TESTS_CCM_INCLUDE_FILES
      INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES INTEGRATION_TESTS_POLICIES_INCLUDE_FILES
      INTEGRATION_TESTS_VALUES_INCLUDE_FILES INTEGRATION_TESTS_SOURCE_FILES
      INTEGRATION_TESTS_CCM_SOURCE_FILES INTEGRATION_TESTS_OBJECTS_SOURCE_FILES
      INTEGRATION_TESTS_TESTS_SOURCE_FILES)
    set(${var_name} ${CASS_${var_name}})
  endforeach()

  GtestIntegrationTestSourceGroups()
  GtestCommonIntegrationTestSourceFiles()
  GtestLibssh2IntegrationTest()
  add_executable(${INTEGRATION_TESTS_NAME}
    ${CASS_API_HEADER_FILES}
    ${COMMON_INTEGRATION_TEST_INCLUDE_FILES}
    ${COMMON_INTEGRATION_TEST_SOURCE_FILES}
    ${INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES}
    ${RAPIDJSON_HEADER_FILES})
  if(CMAKE_VERSION VERSION_LESS "2.8.11")
    include_directories(${INTEGRATION_TESTS_SOURCE_DIR})
    include_directories(${INTEGRATION_TESTS_SOURCE_DIR}/ccm)
  else()
    target_include_directories(${INTEGRATION_TESTS_NAME} PUBLIC ${INTEGRATION_TESTS_SOURCE_DIR})
    target_include_directories(${INTEGRATION_TESTS_NAME} PUBLIC ${INTEGRATION_TESTS_SOURCE_DIR}/ccm)
  endif()
  target_link_libraries(${INTEGRATION_TESTS_NAME}
    ${CASS_LIBS}
    ${PROJECT_LIB_NAME_TARGET}
    ${INTEGRATION_TESTS_LIBSSH2_LIBRARIES})
  # Enable bigobj for large object files during compilation (Cassandra types integration test)
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set(TEST_CMAKE_CXX_FLAGS "${TEST_CMAKE_CXX_FLAGS} /bigobj")
  endif()
  set_target_properties(${INTEGRATION_TESTS_NAME} PROPERTIES PROJECT_LABEL "${INTEGRATION_TESTS_DISPLAY_NAME}"
                                                             FOLDER "Tests"
                                                             COMPILE_FLAGS "${TEST_CMAKE_CXX_FLAGS}")
  if(LIBSSH2_LIBRARY_NAME) # Handle ExternalProject dependency
    add_dependencies(${INTEGRATION_TESTS_NAME} ${LIBSSH2_LIBRARY_NAME})
  endif()

  configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR} COPYONLY)
  if(WIN32)
    # Copy the embedded ADS to additional locations for use with IDE
    if(NOT EXISTS ${CMAKE_BINARY_DIR}/tests)
      file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
    endif()
    configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR}/tests COPYONLY)
  endif()
endif()

#------------------------------
# Unit test executable
#------------------------------
if(CASS_BUILD_UNIT_TESTS)
  # Add the ability to create zip files inside the unit tests
  set(MINIZIP_INCLUDE_DIR "${CASS_SRC_DIR}/third_party/minizip")
  set(MINIZIP_SOURCE_FILES ${MINIZIP_INCLUDE_DIR}/ioapi.c
                           ${MINIZIP_INCLUDE_DIR}/zip.c)

  GtestUnitTests("${MINIZIP_SOURCE_FILES}" "${MINIZIP_INCLUDE_DIR}" "${CASS_EXCLUDED_UNIT_TEST_FILES}")
endif()
