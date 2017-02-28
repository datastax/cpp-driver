cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

# Assign the project settings
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_UNIT_TESTS_NAME ${PROJECT_NAME_STR}_unit_tests)

# Gather the header and source files
file(GLOB UNIT_TESTS_INC_FILES ${PROJECT_SOURCE_DIR}/test/unit_tests/src/*.hpp)
file(GLOB UNIT_TESTS_SRC_FILES ${PROJECT_SOURCE_DIR}/test/unit_tests/src/*.cpp)

# Build up the include paths
set(UNIT_TESTS_INCLUDES ${PROJECT_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/src
  ${CASS_INCLUDES}
  ${Boost_INCLUDE_DIRS}
  ${LIBUV_INCLUDE_DIR})

# Assign the include directories
include_directories(${UNIT_TESTS_INCLUDES})

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${UNIT_TESTS_SRC_FILES})
source_group("Header Files" FILES ${UNIT_TESTS_INC_FILES})

# Build unit tests
add_executable(${PROJECT_UNIT_TESTS_NAME} ${UNIT_TESTS_SRC_FILES})
target_link_libraries(${PROJECT_UNIT_TESTS_NAME} ${PROJECT_LIB_NAME_STATIC} ${CASS_LIBS} ${CASS_TEST_LIBS})
set_property(
  TARGET ${PROJECT_UNIT_TESTS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${CASS_TEST_CXX_FLAGS})
set_property(
  TARGET ${PROJECT_UNIT_TESTS_NAME}
  APPEND PROPERTY LINK_FLAGS ${PROJECT_CXX_LINKER_FLAGS})
