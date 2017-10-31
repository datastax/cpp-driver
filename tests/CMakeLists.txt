##
# Copyright (c) DataStax, Inc.
#
# This software can be used solely with DataStax Enterprise. Please consult the
# license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
##

include(Gtest)

GtestOptions()

# Build the base include paths for the tests
set(CASS_TESTS_ROOT "${CASS_ROOT_DIR}/gtests")
set(CASS_TESTS_SOURCE_DIR "${CASS_TESTS_ROOT}/src")
set(TESTS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(VENDOR_DIR "${CASS_TESTS_SOURCE_DIR}/vendor")
include_directories(${VENDOR_DIR})

GtestSourceGroups()

source_group("Header Files\\driver" FILES ${DSE_API_HEADER_FILES})

#------------------------------
# Google test framework
#------------------------------
GtestFramework()

#------------------------------
# RapidJSON
#------------------------------
CassRapidJson()

#------------------------------
# Integration test executable
#------------------------------
if(DSE_BUILD_INTEGRATION_TESTS)
  CassCcmBridge()
  set(INTEGRATION_TESTS_NAME "${PROJECT_NAME_STRING}-integration-tests")
  set(INTEGRATION_TESTS_DISPLAY_NAME "Integration Tests (${PROJECT_NAME_STRING})")

  GtestIntegrationTestFiles("${TESTS_SOURCE_DIR}/integration" "DSE")
  GtestIntegrationTestFiles("${CASS_TESTS_SOURCE_DIR}/integration" "CASS")

  # Merge the CASS_* variables with DSE_* variables to make the final
  # non-prefixed variables, except for the actual Cassandra integration
  # test files (e.g. not CASS_INTEGRATION_TESTS_TESTS_SOURCE_FILES).
  # Those tests are built into their own test program in core.
  foreach(var_name INTEGRATION_TESTS_INCLUDE_FILES INTEGRATION_TESTS_OBJECTS_INCLUDE_FILES
      INTEGRATION_TESTS_POLICIES_INCLUDE_FILES INTEGRATION_TESTS_VALUES_INCLUDE_FILES
      INTEGRATION_TESTS_SOURCE_FILES INTEGRATION_TESTS_OBJECTS_SOURCE_FILES)
    set(${var_name} ${DSE_${var_name}} ${CASS_${var_name}})
  endforeach()
  set(INTEGRATION_TESTS_TESTS_SOURCE_FILES ${DSE_INTEGRATION_TESTS_TESTS_SOURCE_FILES})

  set(INTEGRATION_TESTS_SOURCE_DIR "${TESTS_SOURCE_DIR}/integration")
  set(CASS_INTEGRATION_TESTS_SOURCE_DIR "${CASS_TESTS_SOURCE_DIR}/integration")

  # We don't want the main.cpp from core integration tests.
  list(REMOVE_ITEM INTEGRATION_TESTS_SOURCE_FILES "${CASS_INTEGRATION_TESTS_SOURCE_DIR}/main.cpp")

  GtestIntegrationTestSourceGroups()

  include_directories(${CASS_INTEGRATION_TESTS_SOURCE_DIR})

  GtestCommonIntegrationTestSourceFiles()
  add_executable(${INTEGRATION_TESTS_NAME}
    ${DSE_API_HEADER_FILES}
    ${RAPIDJSON_HEADER_FILES}
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
    ${DSE_LIBS}
    ${PROJECT_LIB_NAME_TARGET})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY PROJECT_LABEL ${INTEGRATION_TESTS_DISPLAY_NAME})
  set_property(TARGET ${INTEGRATION_TESTS_NAME} PROPERTY FOLDER "Tests")
  set_property(TARGET ${INTEGRATION_TESTS_NAME} APPEND PROPERTY COMPILE_FLAGS ${TEST_CXX_FLAGS})

  configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR} COPYONLY)
  if(WIN32)
    # Copy the embedded ADS to additional locations for use with IDE
    configure_file(embedded-ads.jar ${CMAKE_BINARY_DIR}/tests COPYONLY)
  endif()
endif()

#------------------------------
# Unit test executable
#------------------------------
if(DSE_BUILD_UNIT_TESTS)
  set(DSE_EXTRAS ${DSE_API_HEADER_FILES} ${DSE_INCLUDES})
  GtestUnitTests("dse" "${DSE_EXTRAS}")
endif()
