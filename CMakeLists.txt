cmake_minimum_required(VERSION 2.6.4)
set(PROJECT_NAME_STR cql)
set(CQL_DRIVER_PROJECT_NAME ${PROJECT_NAME_STR})
project(${PROJECT_NAME_STR} C CXX)

#-------------------
# The version number
#-------------------
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 7)
set(PROJECT_VERSION_PATCH 0)
set(PROJECT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

# todo: add version header

#-------------------
# setup deps
#-------------------
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_STATIC_RUNTIME OFF)

# Disable boost auto linking feature
# it prevents visual studio from simultaneously building
# static and shared version of driver
# needed by boost to link dynamically
# this is a noop on platforms other than windows
add_definitions(-DBOOST_ALL_DYN_LINK)
add_definitions(-DBOOST_ALL_NO_LIB)

set(Boost_USE_MULTITHREADED ON)
find_package(Boost 1.41.0 COMPONENTS system thread unit_test_framework date_time program_options REQUIRED)
set(LIBS ${LIBS} ${Boost_LIBRARIES})

find_package(OpenSSL REQUIRED)
set(LIBS ${LIBS} ${OPENSSL_LIBRARIES})

find_package(ZLIB REQUIRED)
set(LIBS ${LIBS} ${ZLIB_LIBRARIES})

# Threading
find_package(Threads REQUIRED)
set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})

# Build up the include path's
set(INCLUDES ${INCLUDES} ${OPENSSL_INCLUDE_DIR} )
set(INCLUDES ${INCLUDES} ${ZLIB_INCLUDE_DIR} )
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS} )

set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})

set(MULTICORE_CORES_NUMBER "3" CACHE STRING "Number of cores (for multicore compilation)")
option(MULTICORE_COMPILATION "Enable multicore compilation" OFF)

if(MULTICORE_COMPILATION)
    # MULTICORE BUILD
    # by default we use 3 cores
    if(MSVC)
        add_definitions("/MP${MULTICORE_CORES_NUMBER}")
    endif()
    # on linux platform this parameter is named -j e.g. -j 5
    # but should be passed to MAKE program not to compiler!
    # You can do this by settings MAKEFLAGS env variable to
    # -j 5. On ubuntu box this can be done by editing
    # ~/.pam_environment file (in any case check man).
endif()

if (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    # On Visual C++ -pedantic flag is not used,
    # -fPIC is not used on Windows platform (all DLLs are
    # relocable), -Wall generates about 30k stupid warnings
    # that can hide useful ones.
    set(PLATFORM_DEPENDENT_COMPILER_OPTIONS "-D_CRT_SECURE_NO_WARNINGS")

    # needed for windows sockets (not verified)
    add_definitions(-D_WIN32_WINNT=0x0501)
else()
	# GCC specific compiler options
        # I disabled long-long warning because boost generates about 50 such warnings
	set(PLATFORM_DEPENDENT_COMPILER_OPTIONS "-fPIC -Wall -pedantic -Wno-long-long -fno-strict-aliasing")
endif()

#-------------------
# Cassandra static and shared
#-------------------

set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

message(STATUS "info CMAKE_BINARY_DIR: ${CMAKE_BINARY_DIR}")

include_directories(${INCLUDES})

# we must add header files as dependencies (if header
# changes project must be recompiled, right).
file(GLOB INC_FILES
	${PROJECT_SOURCE_DIR}/include/cql/*.hpp )

file(GLOB INC_FILES_EXCEPTIONS
  ${PROJECT_SOURCE_DIR}/include/cql/exceptions/*.hpp )

file(GLOB INC_FILES_INTERNAL
	${PROJECT_SOURCE_DIR}/include/cql/internal/*.hpp )

file(GLOB INC_FILES_POLICIES
	${PROJECT_SOURCE_DIR}/include/cql/policies/*.hpp )

file(GLOB SRC_FILES
	${PROJECT_SOURCE_DIR}/src/cql/*.cpp)

file(GLOB SRC_FILES_EXCEPTIONS
	${PROJECT_SOURCE_DIR}/src/cql/exceptions/*.cpp )

file(GLOB SRC_FILES_INTERNAL
	${PROJECT_SOURCE_DIR}/src/cql/internal/*.cpp )

file(GLOB SRC_FILES_POLICIES
	${PROJECT_SOURCE_DIR}/src/cql/policies/*.cpp )

source_group("Source Files" FILES ${SRC_FILES})
source_group("Source Files\\Internal" FILES ${SRC_FILES_INTERNAL})
source_group("Source Files\\Exceptions" FILES ${SRC_FILES_EXCEPTIONS})
source_group("Source Files\\Policies" FILES ${SRC_FILES_POLICIES})

source_group("Header Files" FILES ${INC_FILES})
source_group("Header Files\\Internal" FILES ${INC_FILES_INTERNAL})
source_group("Header Files\\Exceptions" FILES ${INC_FILES_EXCEPTIONS})
source_group("Header Files\\Policies" FILES ${INC_FILES_POLICIES})

set(ALL_SOURCE_FILES
	${SRC_FILES} ${SRC_FILES_INTERNAL} ${SRC_FILES_EXCEPTIONS} ${SRC_FILES_POLICIES}
	${INC_FILES} ${INC_FILES_INTERNAL} ${INC_FILES_EXCEPTIONS} ${INC_FILES_POLICIES})

# build dynamic and static version of library
add_library(${PROJECT_LIB_NAME} SHARED ${ALL_SOURCE_FILES})

set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")
add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${ALL_SOURCE_FILES})

target_link_libraries(${PROJECT_LIB_NAME} ${LIBS})
target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${LIBS})

set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})

set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})

set(PROJECT_COMPILER_FLAGS "${CMAKE_CXX_FLAGS} ${PLATFORM_DEPENDENT_COMPILER_OPTIONS}")

set_property(
 TARGET ${PROJECT_LIB_NAME}
 APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

set_property(
  TARGET ${PROJECT_LIB_NAME_STATIC}
  APPEND PROPERTY COMPILE_FLAGS ${PROJECT_COMPILER_FLAGS})

#-------------------
# install target
#-------------------

# Where to put headers
set(INSTALL_HEADERS_DIR "${CMAKE_INSTALL_PREFIX}/include/cql")
install(FILES ${INC_FILES} DESTINATION "${INSTALL_HEADERS_DIR}")
install(FILES ${INC_FILES_EXCEPTIONS} DESTINATION "${INSTALL_HEADERS_DIR}/exceptions")
install(FILES ${INC_FILES_POLICIES} DESTINATION "${INSTALL_HEADERS_DIR}/policies")

# Where to put libraries
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib")

# For windows only
set(INSTALL_DLL_DIR "${CMAKE_INSTALL_PREFIX}/bin")

install(TARGETS ${PROJECT_LIB_NAME}
  RUNTIME DESTINATION ${INSTALL_DLL_DIR}  # for dll files
  LIBRARY DESTINATION ${INSTALL_LIB_DIR}  # for shared library
  ARCHIVE DESTINATION ${INSTALL_LIB_DIR}) # for static library

install(TARGETS ${PROJECT_LIB_NAME_STATIC}
  RUNTIME DESTINATION ${INSTALL_DLL_DIR}  # for dll files
  LIBRARY DESTINATION ${INSTALL_LIB_DIR}  # for shared library
  ARCHIVE DESTINATION ${INSTALL_LIB_DIR}) # for static library

#-------------------
# uninstall target
#-------------------

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(UNINSTALL
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)


#-------------------
# Boost unit tests
#-------------------
add_subdirectory(test/unit_tests)

#-------------------
# Integration tests
#-------------------
add_subdirectory(extra/ccm_bridge)
add_subdirectory(test/integration_tests)

#-------------------
# the demo program
#-------------------
add_subdirectory(demo)
