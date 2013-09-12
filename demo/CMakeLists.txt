#-------------------
# Demo
#-------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(DEMO Demo_${CQL_DRIVER_PROJECT_NAME})

project(${DEMO})



file(GLOB DEMO_SRC_FILES *.cpp)
add_executable(${DEMO} EXCLUDE_FROM_ALL ${DEMO_SRC_FILES})

set_property(
	TARGET ${TEST}
	APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

# Setup include directories 
# Directories are inherited from parent project 
set(CCM_BRIDGE_INCLUDES "${CMAKE_PROJECT_SOURCE}/extra/ccm_bridge/include")
target_include_directories(${DEMO} PUBLIC ${INCLUDES} ${CCM_BRIDGE_INCLUDES})
target_link_libraries(${DEMO} ${LIBS} ${PROJECT_LIB_NAME_STATIC} CCMBridge)

set_property(
  TARGET ${DEMO}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

add_dependencies(${DEMO} ${PROJECT_LIB_NAME_STATIC})
add_dependencies(${DEMO} CCMBridge)