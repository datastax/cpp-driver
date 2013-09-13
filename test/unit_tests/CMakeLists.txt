cmake_minimum_required(VERSION 2.8.11)

set(TEST UnitTests_${CQL_DRIVER_PROJECT_NAME})
project(${TEST} C CXX)

# ---------------------------------------------------------------
# SPECIFY TARGET

# Setup files that are part of unit tests
file(GLOB SRC_FILES *.cpp)
add_executable(${TEST} ${SRC_FILES})

set_property(
	TARGET ${TEST}
	APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

# Setup include directories 
# Directories are inherited from parent project 
set(CCM_BRIDGE_INCLUDES "${CMAKE_PROJECT_SOURCE}/extra/ccm_bridge/include")
target_include_directories(${TEST} PUBLIC ${INCLUDES} ${CCM_BRIDGE_INCLUDES})

target_link_libraries(${TEST} 
	${LIBS} ${PROJECT_LIB_NAME_STATIC} ${PLATFORM_DEPENDENT_LIBS} CCMBridge)

# ---------------------------------------------------------------

# ---------------------------------------------------------------
# SPECIFY TEST DEPENDENCIES

# Dependency to libcql
add_dependencies(${TEST} ${PROJECT_LIB_NAME_STATIC})

# Dependency to ccm_bridge
add_dependencies(${TEST} CCMBridge)

# ----------------------------------------------------------------

# ----------------------------------------------------------------
# TEST WITH CTEST FRAMEWORK

enable_testing()
add_test(${TEST} ${TEST})
# ----------------------------------------------------------------
