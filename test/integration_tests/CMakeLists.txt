#-------------------
# Integration tests
#-------------------
cmake_minimum_required(VERSION 2.6.4)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_INTTESTS_NAME ${PROJECT_NAME_STR}_integration_tests)

set(CCM_BRIDGE_INCLUDES "${PROJECT_SOURCE_DIR}/extra/ccm_bridge/include")

set(PROJECT_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/test/integration_tests/include")
file(GLOB INTTESTS_INC_FILES ${PROJECT_INCLUDE_DIR}/*.hpp)
file(GLOB INTTESTS_SRC_FILES ${PROJECT_SOURCE_DIR}/test/integration_tests/src/*.cpp)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})

# Create source groups - this is only needed to create
# neat folder structure in Visual Studio...
source_group("Source Files" FILES ${INTTESTS_SRC_FILES})
source_group("Header Files" FILES ${INTTESTS_INC_FILES})

include_directories(${INCLUDES} ${CCM_BRIDGE_INCLUDES})
add_executable(${PROJECT_INTTESTS_NAME} ${INTTESTS_SRC_FILES} ${INTTESTS_INC_FILES})
target_link_libraries(${PROJECT_INTTESTS_NAME} ${LIBS} ${PROJECT_LIB_NAME} CCMBridge)
	
set_property(
  TARGET ${PROJECT_INTTESTS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})
