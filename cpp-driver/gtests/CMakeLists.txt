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
set(TESTS_SCASSANDRA_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration/scassandra")
set(TESTS_SCASSANDRA_SERVER_SOURCE_DIR "${TESTS_SCASSANDRA_SOURCE_DIR}/server")
set(VENDOR_DIR "${TESTS_SOURCE_DIR}/vendor")
include_directories(${VENDOR_DIR})

GtestSourceGroups()

#------------------------------
# Google test framework
#------------------------------
GtestFramework()

#------------------------------
# Integration test executable
#------------------------------
if(CASS_BUILD_INTEGRATION_TESTS)
  CassSCassandra()
  CassCcmBridge()
  CassRapidJson()

  set(INTEGRATION_TESTS_NAME "cassandra-integration-tests")
  set(INTEGRATION_TESTS_DISPLAY_NAME "Integration Tests (cassandra)")
  set(INTEGRATION_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration")

  GtestIntegrationTestFiles(${INTEGRATION_TESTS_SOURCE_DIR} "CASS")

  # Promote the CASS_* variables to be non-prefixed "final" variables.
  foreach(var_name INTEGRATION_TESTS_INCLUDE_FILES INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES
      INTEGRATION_TESTS_POLICIES_INCLUDE_FILES INTEGRATION_TESTS_VALUES_INCLUDE_FILES
      INTEGRATION_TESTS_SOURCE_FILES INTEGRATION_TESTS_OBJECTS_SOURCE_FILES
      INTEGRATION_TESTS_TESTS_SOURCE_FILES)
    set(${var_name} ${CASS_${var_name}})
  endforeach()

  if (EXISTS ${SCASSANDRA_SERVER_STANDALONE_JAR})
    # Configure the integration tests to use SCassandra
    add_definitions(-DDSE_USE_STANDALONE_SCASSANDRA_SERVER)
    get_filename_component(SCASSANDRA_SERVER_STANDALONE_JAR_BASENAME
        ${SCASSANDRA_SERVER_STANDALONE_JAR} NAME)
    add_definitions(-DSCASSANDRA_SERVER_JAR="${SCASSANDRA_SERVER_STANDALONE_JAR_BASENAME}")
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES ${TESTS_SCASSANDRA_SOURCE_DIR}/*.hpp)
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES ${TESTS_SCASSANDRA_SOURCE_DIR}/*.cpp)
    file(GLOB INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/tests/scassandra/*.cpp)
    source_group("Header Files\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES})
    source_group("Source Files\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES})
    source_group("Source Files\\tests\\scassandra" FILES ${INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES})
    include_directories(${TESTS_SCASSANDRA_SOURCE_DIR})
  endif()

  GtestIntegrationTestSourceGroups()
  GtestCommonIntegrationTestSourceFiles()
  add_executable(${INTEGRATION_TESTS_NAME}
    ${INTEGRATION_TESTS_SCASSANDRA_SOURCE_FILES}
    ${INTEGRATION_TESTS_SCASSANDRA_TESTS_SOURCE_FILES}
    ${INTEGRATION_TESTS_SCASSANDRA_INCLUDE_FILES}
    ${COMMON_INTEGRATION_TEST_SOURCE_FILES}
  )
  if(CMAKE_VERSION VERSION_LESS "2.8.11")
    include_directories(${INTEGRATION_TESTS_SOURCE_DIR})
  else()
    target_include_directories(${INTEGRATION_TESTS_NAME} PUBLIC ${INTEGRATION_TESTS_SOURCE_DIR})
  endif()
  target_link_libraries(${INTEGRATION_TESTS_NAME}
    ${CCM_BRIDGE_LIBRARIES}
    ${GOOGLE_TEST_LIBRARIES}
    ${CASS_LIBS}
    ${PROJECT_LIB_NAME_TARGET})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY PROJECT_LABEL ${INTEGRATION_TESTS_DISPLAY_NAME})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY FOLDER "Tests")
  set_property(TARGET ${INTEGRATION_TESTS_NAME} APPEND PROPERTY COMPILE_FLAGS ${TEST_CXX_FLAGS})

  # Copy the test related dependencies to their proper locations
  if(EXISTS ${SCASSANDRA_SERVER_STANDALONE_JAR})
    # Copy the standalone SCassandra server JAR file to output directory
    configure_file(${SCASSANDRA_SERVER_STANDALONE_JAR} ${CMAKE_BINARY_DIR} COPYONLY)
    if(WIN32)
      # Copy SCassandra server to additional locations for use with IDE
      get_filename_component(SCASSANDRA_SERVER_STANDALONE_JAR_FILENAME
          ${SCASSANDRA_SERVER_STANDALONE_JAR}
          NAME)
      configure_file(${SCASSANDRA_SERVER_STANDALONE_JAR} ${CMAKE_BINARY_DIR}/tests COPYONLY)
    endif()
  endif()
endif()

#------------------------------
# Unit test executable
#------------------------------
if(CASS_BUILD_UNIT_TESTS)
  GtestUnitTests("cassandra" "")
endif()
