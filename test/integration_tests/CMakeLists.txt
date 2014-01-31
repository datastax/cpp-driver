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
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})

# Create source groups - this is only needed to create
# neat folder structure in Visual Studio...
source_group("Source Files" FILES ${INTTESTS_SRC_FILES})
source_group("Header Files" FILES ${INTTESTS_INC_FILES})

include_directories(${INCLUDES} ${CCM_BRIDGE_INCLUDES})
add_executable(${PROJECT_INTTESTS_NAME} ${INTTESTS_SRC_FILES} ${INTTESTS_INC_FILES})

# A temporary patch: explicitly link libssh2
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/extra/ccm_bridge/cmake/Modules/")
find_package(LIBSSH2 REQUIRED)
target_link_libraries(${PROJECT_INTTESTS_NAME} ${PROJECT_LIB_NAME} CCMBridge ${LIBS} ${LIBSSH2_LIBRARIES})
	
set_property(
  TARGET ${PROJECT_INTTESTS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})
