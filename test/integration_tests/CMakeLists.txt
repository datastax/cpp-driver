cmake_minimum_required(VERSION 2.6.4)

include(Gtest)
CassCcmBridge()

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

# Assign the project settings
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_INTEGRATION_TESTS_NAME cassandra_integration_tests)

# Gather the header and source files
file(GLOB INTEGRATION_TESTS_INC_FILES ${CASS_ROOT_DIR}/test/integration_tests/src/*.hpp)
file(GLOB INTEGRATION_TESTS_SRC_FILES ${CASS_ROOT_DIR}/test/integration_tests/src/*.cpp)

# Build up the include paths
set(INTEGRATION_TESTS_INCLUDES ${PROJECT_INCLUDE_DIR}
  ${CASS_SRC_DIR}
  ${CASS_INCLUDES}
  ${Boost_INCLUDE_DIRS}
  ${CCM_BRIDGE_INCLUDES}
  ${LIBSSH2_INCLUDE_DIRS}
  ${LIBUV_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR})

# Assign the include directories
include_directories(${INTEGRATION_TESTS_INCLUDES})

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${INTEGRATION_TESTS_SRC_FILES})
source_group("Header Files" FILES ${INTEGRATION_TESTS_INC_FILES})

# Build integreation tests
add_definitions(-DBOOST_TEST_IGNORE_NON_ZERO_CHILD_CODE) # Ignore system calls that might have a non-zero exit code
add_executable(${PROJECT_INTEGRATION_TESTS_NAME}
  ${CCM_BRIDGE_HEADER_FILES} ${CCM_BRIDGE_SOURCE_FILES}
  ${INTEGRATION_TESTS_INC_FILES} ${INTEGRATION_TESTS_SRC_FILES})
target_link_libraries(${PROJECT_INTEGRATION_TESTS_NAME}
  ${PROJECT_LIB_NAME_TARGET}
  ${CCM_BRIDGE_LIBRARIES}
  ${CASS_LIBS}
  ${CASS_TEST_LIBS})
set_property(
  TARGET ${PROJECT_INTEGRATION_TESTS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${CASS_TEST_CXX_FLAGS})
set_property(
  TARGET ${PROJECT_INTEGRATION_TESTS_NAME}
  APPEND PROPERTY LINK_FLAGS ${PROJECT_CXX_LINKER_FLAGS})
