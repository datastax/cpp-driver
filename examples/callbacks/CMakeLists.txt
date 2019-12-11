cmake_minimum_required(VERSION 2.6.4)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_EXAMPLE_NAME callbacks)

file(GLOB EXAMPLE_SRC_FILES ${CASS_ROOT_DIR}/examples/callbacks/*.c)
include_directories(${INCLUDES})
add_executable(${PROJECT_EXAMPLE_NAME} ${EXAMPLE_SRC_FILES})
target_link_libraries(${PROJECT_EXAMPLE_NAME} ${PROJECT_LIB_NAME_TARGET} ${CASS_LIBS})
add_dependencies(${PROJECT_EXAMPLE_NAME} ${PROJECT_LIB_NAME_TARGET})

set_target_properties(${PROJECT_EXAMPLE_NAME} PROPERTIES FOLDER "Examples"
                                                         COMPILE_FLAGS "${EXAMPLE_CMAKE_C_FLAGS}")
