#------------------------------
# Integration test executable
#------------------------------

if(CASS_USE_LIBSSH2)
  # Allow for libssh2 directory to be specified on the command line
  if(NOT LIBSSH2_ROOT)
    if(EXISTS "${PROJECT_SOURCE_DIR}/lib/libssh2/")
      set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/")
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/build/libs/libssh2/")
      set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/build/libs/libssh2/")
    elseif(DEFINED ENV{LIBSSH2_ROOT} AND
        EXISTS "$ENV{LIBSSH2_ROOT}")
      set(LIBSSH2_ROOT "$ENV{LIBSSH2_ROOT}")
    endif()
  endif()
  if(LIBSSH2_ROOT_DIR)
    if(EXISTS ${LIBSSH2_ROOT_DIR})
      set(LIBSSH2_ROOT ${LIBSSH2_ROOT_DIR})
    endif()
  endif()

  # Check for libssh2 availability (build if necessary on Windows)
  if(WIN32 AND LIBSSH2_VERSION) # Store the current version of libssh2 to prevent corruption
    set(SAVED_LIBSSH2_VERSION ${LIBSSH2_VERSION})
  endif()
  find_package(Libssh2)
  if(WIN32 AND NOT LIBSSH2_FOUND)
    message(STATUS "Unable to Locate libssh2: Third party build step will be performed")
    if(SAVED_LIBSSH2_VERSION)
      set(LIBSSH2_VERSION ${SAVED_LIBSSH2_VERSION})
    endif()
    include(ExternalProject-libssh2)
  elseif(NOT LIBSSH2_FOUND)
    message(STATUS "libssh2 is Unavailable: Building integration tests without libssh2 support")
  endif()
endif()

if(LIBSSH2_FOUND OR LIBSSH2_INCLUDE_DIRS)
  include_directories(${LIBSSH2_INCLUDE_DIRS})
  set(INTEGRATION_TESTS_LIBSSH2_LIBRARIES ${LIBSSH2_LIBRARIES})
  # Build up the includes and libraries for CCM dependencies
  add_definitions(-DCASS_USE_LIBSSH2 -DOPENSSL_CLEANUP)
  file(GLOB INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES ${LIBSSH2_INCLUDE_DIRS}/*.h)
  source_group("Header Files\\libssh2" FILES ${INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES})

  if(WIN32)
    add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
  endif()
endif()

add_executable(cassandra-integration-tests
  ${CASS_API_HEADER_FILES}
  ${COMMON_INTEGRATION_TEST_INCLUDE_FILES}
  ${COMMON_INTEGRATION_TEST_SOURCE_FILES}
  ${INTEGRATION_TESTS_LIBSSH2_INCLUDE_FILES}
  ${RAPIDJSON_HEADER_FILES})

target_include_directories(cassandra-integration-tests PRIVATE 
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/ccm
  ${CASS_INCLUDES})

target_link_libraries(cassandra-integration-tests
  ${CASS_LIBS}
  ${PROJECT_LIB_NAME_TARGET}
  ${INTEGRATION_TESTS_LIBSSH2_LIBRARIES})

set_target_properties(cassandra-integration-tests PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Enable bigobj for large object files during compilation (Cassandra types integration test)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  set(TEST_CMAKE_CXX_FLAGS "${TEST_CMAKE_CXX_FLAGS} /bigobj")
endif()

set_target_properties(cassandra-integration-tests PROPERTIES
  PROJECT_LABEL  "Integration Tests"
  FOLDER "Tests"
  COMPILE_FLAGS "${TEST_CMAKE_CXX_FLAGS}")

if(LIBSSH2_LIBRARY_NAME) # Handle ExternalProject dependency
  add_dependencies(cassandra-integration-tests ${LIBSSH2_LIBRARY_NAME})
endif()
