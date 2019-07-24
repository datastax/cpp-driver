cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

# Assign the project settings
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ".")
set(PROJECT_INTEGRATION_TESTS_NAME cassandra_integration_tests)

# Gather the header and source files
file(GLOB INTEGRATION_TESTS_INC_FILES ${CASS_ROOT_DIR}/test/integration_tests/src/*.hpp)
file(GLOB INTEGRATION_TESTS_SRC_FILES ${CASS_ROOT_DIR}/test/integration_tests/src/*.cpp)

# Build up the include paths
set(INTEGRATION_TESTS_INCLUDES ${PROJECT_INCLUDE_DIR}
                               ${CASS_SRC_DIR}
                               ${CASS_INCLUDES}
                               ${Boost_INCLUDE_DIRS}
                               ${CCM_BRIDGE_INCLUDES}
                               ${LIBUV_INCLUDE_DIR}
                               ${OPENSSL_INCLUDE_DIR})

# Assign the include directories
include_directories(${INTEGRATION_TESTS_INCLUDES})

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${INTEGRATION_TESTS_SRC_FILES})
source_group("Header Files" FILES ${INTEGRATION_TESTS_INC_FILES})

# Build integreation tests
add_definitions(-DBOOST_TEST_IGNORE_NON_ZERO_CHILD_CODE) # Ignore system calls that might have a non-zero exit code
if(LIBSSH2_FOUND)
  add_definitions(-DCASS_USE_LIBSSH2)
endif()
add_executable(${PROJECT_INTEGRATION_TESTS_NAME}
               ${CCM_BRIDGE_HEADER_FILES}
               ${CCM_BRIDGE_SOURCE_FILES}
               ${INTEGRATION_TESTS_INC_FILES}
               ${INTEGRATION_TESTS_SRC_FILES})
target_link_libraries(${PROJECT_INTEGRATION_TESTS_NAME}
  general ${PROJECT_LIB_NAME_TARGET}
          CCMBridge
          ${CCM_BRIDGE_LIBRARIES}
          ${CASS_LIBS}
  # Allow linking of different Boost libraries
  optimized ${Boost_CHRONO_LIBRARY_RELEASE}
            ${Boost_SYSTEM_LIBRARY_RELEASE}
            ${Boost_THREAD_LIBRARY_RELEASE}
            ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY_RELEASE}
  debug ${Boost_CHRONO_LIBRARY_DEBUG}
        ${Boost_SYSTEM_LIBRARY_DEBUG}
        ${Boost_THREAD_LIBRARY_DEBUG}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY_DEBUG})
if(UNIX)
  target_link_libraries(${PROJECT_INTEGRATION_TESTS_NAME} pthread) # test_basics requires pthread
endif()
if(BOOST_LIBRARY_NAME)
  add_dependencies(${PROJECT_INTEGRATION_TESTS_NAME} ${BOOST_LIBRARY_NAME})
endif()
if(LIBSSH2_LIBRARY_NAME)
  add_dependencies(${PROJECT_INTEGRATION_TESTS_NAME} ${LIBSSH2_LIBRARY_NAME})
endif()
