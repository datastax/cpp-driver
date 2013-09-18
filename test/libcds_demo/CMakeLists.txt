#-------------------
# Demo
#-------------------
cmake_minimum_required(VERSION 2.8.11)

set(LIBCDS_DEMO libcds_demo)
project(${LIBCDS_DEMO} C CXX)

file(GLOB LIBCDS_DEMO_SRC_FILES *.cpp)
add_executable(${LIBCDS_DEMO} ${LIBCDS_DEMO_SRC_FILES})

set_property(
  TARGET ${LIBCDS_DEMO}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

target_include_directories( ${LIBCDS_DEMO} PUBLIC ${INCLUDES})
target_link_libraries( ${LIBCDS_DEMO} ${LIBS})
