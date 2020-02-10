#------------------------------
# Unit test executable
#------------------------------

set(MINIZIP_INCLUDE_DIR "${CASS_SRC_DIR}/third_party/minizip")
set(MINIZIP_SOURCE_FILES ${MINIZIP_INCLUDE_DIR}/zip.c) # Zip functionality is not built into the driver

set(HTTP_PARSER_INCLUDE_DIR "${CASS_ROOT_DIR}/src/third_party/http-parser")

# The unit tests use `test::Utils::msleep()` and this is the minimum include
# and source files required to shared that code.
set(INTEGRATION_TESTS_SOURCE_DIR ${CASS_ROOT_DIR}/tests/src/integration)
set(INTEGRATION_TESTS_CCM_SOURCE_DIR ${INTEGRATION_TESTS_SOURCE_DIR}/ccm)
set(INTEGRATION_TESTS_SOURCE_FILES ${INTEGRATION_TESTS_SOURCE_DIR}/test_utils.cpp)
set(INTEGRATION_TESTS_CCM_SOURCE_FILES "${INTEGRATION_TESTS_CCM_SOURCE_DIR}/tsocket.cpp")
file(GLOB UNIT_TESTS_INCLUDE_FILES *.hpp)
file(GLOB UNIT_TESTS_SOURCE_FILES *.cpp)
file(GLOB UNIT_TESTS_TESTS_SOURCE_FILES tests/*.cpp)

source_group("Header Files" FILES ${UNIT_TESTS_INCLUDE_FILES})
source_group("Source Files" FILES ${UNIT_TESTS_SOURCE_FILES})
source_group("Source Files\\tests" FILES ${UNIT_TESTS_TESTS_SOURCE_FILES})

add_executable(cassandra-unit-tests
  ${MINIZIP_SOURCE_FILES}
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

target_include_directories(cassandra-unit-tests PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CASS_INCLUDES}
  ${MINIZIP_INCLUDE_DIR}
  ${HTTP_PARSER_INCLUDE_DIR}
  ${INTEGRATION_TESTS_SOURCE_DIR}
  ${INTEGRATION_TESTS_CCM_SOURCE_DIR})

target_link_libraries(cassandra-unit-tests
  ${CASS_LIBS}
  ${PROJECT_LIB_NAME_TARGET})

set_target_properties(cassandra-unit-tests PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set_target_properties(cassandra-unit-tests PROPERTIES 
  PROJECT_LABEL "Unit Tests"
  FOLDER "Tests")

# Add the unit tests to be executed by ctest (see CMake BUILD_TESTING)
add_test("Unit Tests" cassandra-unit-tests COMMAND cassandra-unit-tests)

set_tests_properties("Unit Tests" PROPERTIES TIMEOUT 5)
