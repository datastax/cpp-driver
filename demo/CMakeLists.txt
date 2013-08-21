#-------------------
# Demo
#-------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_DEMO_NAME ${PROJECT_NAME_STR}_demo)

file(GLOB DEMO_SRC_FILES ${PROJECT_SOURCE_DIR}/demo/*.cpp)
include_directories(${INCLUDES})
add_executable(${PROJECT_DEMO_NAME} ${DEMO_SRC_FILES})
target_link_libraries(${PROJECT_DEMO_NAME} ${LIBS} ${PROJECT_LIB_NAME})

set_property(
  TARGET ${PROJECT_DEMO_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})