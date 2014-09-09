cmake_minimum_required(VERSION 2.8.11)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_EXAMPLE_NAME bind_by_name)

file(GLOB EXAMPLE_SRC_FILES ${PROJECT_SOURCE_DIR}/examples/bind_by_name/*.c)
include_directories(${INCLUDES})
add_executable(${PROJECT_EXAMPLE_NAME} ${EXAMPLE_SRC_FILES})
target_link_libraries(${PROJECT_EXAMPLE_NAME} ${LIBS} ${PROJECT_LIB_NAME})
add_dependencies(${PROJECT_EXAMPLE_NAME} ${PROJECT_LIB_NAME})

set_property(
  TARGET ${PROJECT_EXAMPLE_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${EXAMPLE_C_FLAGS})
