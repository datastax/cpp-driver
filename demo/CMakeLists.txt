#-------------------
# Demo
#-------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(DEMO Demo_${CQL_DRIVER_PROJECT_NAME})

project(${DEMO})

file(GLOB DEMO_SRC_FILES *.cpp)

include_directories(${INCLUDES})

add_executable(${DEMO} EXCLUDE_FROM_ALL ${DEMO_SRC_FILES})

add_dependencies(${DEMO} ${PROJECT_LIB_NAME_STATIC})
target_link_libraries(${DEMO} ${LIBS} ${PROJECT_LIB_NAME_STATIC})

set_property(
  TARGET ${DEMO}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})