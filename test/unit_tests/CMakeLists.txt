cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:LIBCMT")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /NODEFAULTLIB:LIBCMT")
endif()

set(LIBS ${LIBS} ${Boost_LIBRARIES})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_UNIT_TESTS_NAME ${PROJECT_NAME_STR}_unit_tests)

set(CCM_BRIDGE_INCLUDES "${PROJECT_SOURCE_DIR}/test/ccm_bridge/include")

file(GLOB UNIT_TESTS_INC_FILES ${PROJECT_SOURCE_DIR}/test/unit_tests/src/*.hpp)
file(GLOB UNIT_TESTS_SRC_FILES ${PROJECT_SOURCE_DIR}/test/unit_tests/src/*.cpp)

set(INCLUDES ${PROJECT_INCLUDE_DIR})
set(INCLUDES ${INCLUDES} "${PROJECT_SOURCE_DIR}/src")
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})
set(INCLUDES ${INCLUDES} ${LIBUV_INCLUDE_DIR})

# Create source groups - this is only needed to create
# neat folder structure in Visual Studio...
source_group("Source Files" FILES ${UNIT_TESTS_SRC_FILES})
source_group("Header Files" FILES ${UNIT_TESTS_INC_FILES})

include_directories(${INCLUDES} ${CCM_BRIDGE_INCLUDES})
add_executable(${PROJECT_UNIT_TESTS_NAME} ${UNIT_TESTS_SRC_FILES})

# A temporary patch: explicitly link libssh2
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/test/ccm_bridge/cmake/Modules/")
find_package(LIBSSH2 REQUIRED)
target_link_libraries(${PROJECT_UNIT_TESTS_NAME} "${PROJECT_TARGET_LIB_NAME}" CCMBridge ${LIBS} ${LIBSSH2_LIBRARIES})
	
set_property(
  TARGET ${PROJECT_UNIT_TESTS_NAME}
  APPEND PROPERTY COMPILE_FLAGS ${TEST_CXX_FLAGS})
