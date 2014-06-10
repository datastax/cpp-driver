#-------------------
# Test
#-------------------
cmake_minimum_required(VERSION 2.6.4)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_TEST_NAME ${PROJECT_NAME_STR}_unit_tests)

set(TEST ${PROJECT_TEST_NAME})
include_directories(${INCLUDES} ${PROJECT_SOURCE_DIR}/src/)

file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/test/unit_tests/*.cpp)
add_executable(${PROJECT_TEST_NAME} EXCLUDE_FROM_ALL ${TEST_SRC_FILES})
target_link_libraries(${PROJECT_TEST_NAME} ${PROJECT_LIB_NAME_STATIC} ${LIBS})

set_property(
  TARGET ${PROJECT_TEST_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_CXX_FLAGS})

enable_testing()
add_test(${PROJECT_TEST_NAME} ${PROJECT_TEST_NAME})
