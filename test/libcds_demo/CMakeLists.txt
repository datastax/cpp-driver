#-------------------
# Demo
#-------------------
cmake_minimum_required(VERSION 2.8.11)

set(LIBCDS_DEMO libcds_demo)
project(${LIBCDS_DEMO} C CXX)

file(GLOB LIBCDS_DEMO_SRC_FILES *.cpp *.hpp)
add_executable(${LIBCDS_DEMO} ${LIBCDS_DEMO_SRC_FILES})

# change -std=c++03 into -std=c++0x for linux

set(DEFAULT_COMPILER_FLAGS ${PROJECT_COMPILER_FLAGS})
string(REPLACE "-std=c++03" "-std=c++0x" CHANGED_COMPILER_FLAGS ${DEFAULT_COMPILER_FLAGS})
set(PROJECT_COMPILER_FLAGS ${CHANGED_COMPILER_FLAGS})

set_property(
  TARGET ${LIBCDS_DEMO}
  APPEND PROPERTY COMPILE_FLAGS "${PROJECT_COMPILER_FLAGS}")

target_include_directories( ${LIBCDS_DEMO} PUBLIC ${INCLUDES})
target_link_libraries(${LIBCDS_DEMO} ${LIBS})
