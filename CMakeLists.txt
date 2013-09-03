cmake_minimum_required(VERSION 2.8.0)
set(PROJECT_NAME_STR cql)
# Alias for project name used in unit tests
set(CQL_DRIVER_PROJECT_NAME ${PROJECT_NAME_STR})
project(${PROJECT_NAME_STR} C CXX)

#-------------------
# The version number
#-------------------

set (cassandra_VERSION_MAJOR 0)
set (cassandra_VERSION_MINOR 7)
set (cassandra_VERSION_PATCH 0)
set (cassandra_VERSION_STRING ${cassandra_VERSION_MAJOR}.${cassandra_VERSION_MINOR}.${cassandra_VERSION_PATCH})

# todo: add version header

#-------------------
# setup deps
#-------------------

# ${PROJECT_SOURCE_DIR} is not src directory but
# project root directory.
set(EXT_PREFIX ${PROJECT_SOURCE_DIR}/ext)
set(CMAKE_MODULE_PATH ${EXT_PREFIX})

# OS specific lib stuff
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
	set(Boost_USE_STATIC_LIBS ON)
	set(Boost_USE_STATIC_RUNTIME ON)

	include(openssl)
	include(zlib)
	set(LIBS ${LIBS} ${openssl_STATIC_LIBRARIES} )
	set(LIBS ${LIBS} ${zlib_STATIC_LIBRARIES} )
else()
	set(Boost_USE_STATIC_LIBS OFF)
	set(Boost_USE_STATIC_RUNTIME OFF)

	find_package(OpenSSL REQUIRED)
	set(LIBS ${LIBS} ${OPENSSL_LIBRARIES} )
  
	find_package(ZLIB REQUIRED)
	set(LIBS ${LIBS} ${ZLIB_LIBRARIES} )
endif()

# Boost
set(Boost_USE_MULTITHREADED ON)
find_package(Boost 1.54.0 COMPONENTS system thread unit_test_framework date_time REQUIRED)
set(LIBS ${LIBS} ${Boost_LIBRARIES})

# Threading
find_package(Threads REQUIRED)
set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})

# Build up the include path's
set(INCLUDES ${INCLUDES} ${OPENSSL_INCLUDE_DIR} )
set(INCLUDES ${INCLUDES} ${ZLIB_INCLUDE_DIR} )
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS} )

set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})

#!!!!
message("info include_directories: ${INCLUDES}")

#!!!!!!!
message("info target_link_libraries: ${LIBS}")

# OS specific compiler options
set(PLATFORM_DEPENDENT_COMPILER_OPTIONS "-fPIC -Wall -pedantic")

# disable boost auto linking feature
# it prevents visual studio from simultaneously building
# static and shared version of driver
add_definitions(-DBOOST_ALL_NO_LIB )

option(MULTICORE_COMPILATION "Enable multicore compilation" OFF)

if(MULTICORE_COMPILATION)
    # MULTICORE BUILD
    # by default we use 3 cores
    if(MSVC)
        add_definitions(/MP3)
    else()
        add_definitions(-j3)
    endif()
endif()

if (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    # On Visual C++ -pedantic flag is not used,
    # -fPIC is not used on Windows platform (all DLLs are
    # relocable), -Wall generates about 30k stupid warnings
    # that can hide useful ones.
    set(PLATFORM_DEPENDENT_COMPILER_OPTIONS "")

    # needed for windows sockets (not verified)
    add_definitions(-D_WIN32_WINNT=0x0501)
endif()


#-------------------
# Cassandra static and shared
#-------------------

set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

message("info CMAKE_BINARY_DIR: ${CMAKE_BINARY_DIR}")

include_directories(${INCLUDES})

# we must add header files as dependencies (if header
# changes project must be recompiled, right).
file(GLOB INC_FILES
    ${PROJECT_SOURCE_DIR}/include/cassandra/internal/*.hpp 
	${PROJECT_SOURCE_DIR}/include/cassandra/*.hpp) 

file(GLOB SRC_FILES 
    ${PROJECT_SOURCE_DIR}/src/cassandra/internal/*.cpp 
	${PROJECT_SOURCE_DIR}/src/cassandra/*.cpp)

source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${INC_FILES})

# build dynamic and static version of library
add_library(${PROJECT_LIB_NAME} SHARED ${SRC_FILES} ${INC_FILES})

set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")
add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${SRC_FILES} ${INC_FILES})

target_link_libraries(${PROJECT_LIB_NAME} ${LIBS})
target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${LIBS})

set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${cassandra_VERSION_STRING} SOVERSION ${cassandra_VERSION_MAJOR})

set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${cassandra_VERSION_STRING} SOVERSION ${cassandra_VERSION_MAJOR})

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

file(GLOB INSTALL_HEADERS "${PROJECT_INCLUDE_DIR}/cassandra/*.hpp")

# Where to put headers
set(INSTALL_HEADERS_DIR "${CMAKE_INSTALL_PREFIX}/include/casandra")
install(FILES ${INSTALL_HEADERS} DESTINATION "${INSTALL_HEADERS_DIR}")

# Where to put libraries
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib")
# For windows only
set(INSTALL_DLL_DIR "${CMAKE_INSTALL_PREFIX}/bin")
install(TARGETS ${PROJECT_LIB_NAME_STATIC} ${PROJECT_LIB_NAME}
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

enable_testing()
add_subdirectory(test/unit_tests)

#-------------------
# the demo program
#-------------------

add_subdirectory(demo)
