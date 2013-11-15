#-------------------
# Stress
#-------------------
cmake_minimum_required(VERSION 2.6.4)

# set(STRESS Stress_${CQL_DRIVER_PROJECT_NAME})
# project(${STRESS} C CXX)

# file(GLOB STRESS_SRC_FILES *.cpp)

# # Add EXCLUDE_FROM_ALL flag to exclude stress test
# # from ALL target
# add_executable(${STRESS}  ${STRESS_SRC_FILES})

# set_property(
#   TARGET ${STRESS}
#   APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})




set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_STRESS_NAME ${PROJECT_NAME_STR}_stress)

file(GLOB STRESS_SRC_FILES ${PROJECT_SOURCE_DIR}/stress/*.cpp)
include_directories(${INCLUDES})
add_executable(${PROJECT_STRESS_NAME} ${STRESS_SRC_FILES})
target_link_libraries(${PROJECT_STRESS_NAME} ${LIBS} ${PROJECT_LIB_NAME})

set_property(
  TARGET ${PROJECT_STRESS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})