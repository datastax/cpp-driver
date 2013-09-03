
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(TEST UnitTests_${CQL_DRIVER_PROJECT_NAME})
project(${TEST})

include_directories(${INCLUDES})

file(GLOB SRC_FILES *.cpp)
add_executable(${TEST} ${SRC_FILES})

add_dependencies(${TEST} ${PROJECT_LIB_NAME_STATIC})
# add_definitions(-DBOOST_TEST_DYN_LINK) 

# link to STATIC version of cql driver

if (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    # add socket library 
    set(PLATFORM_DEPENDENT_LIBS "Ws2_32.lib")
endif()

target_link_libraries(${TEST} ${PROJECT_LIB_NAME_STATIC} ${LIBS} ${PLATFORM_DEPENDENT_LIBS})

MESSAGE("boost.test link: ${PROJECT_LIB_NAME_STATIC} ${LIBS}")
MESSAGE("boost.test incl: ${INCLUDES}")

set_property(
  TARGET ${TEST}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

add_test(${TEST} ${TEST})
