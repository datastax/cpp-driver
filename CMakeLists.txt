cmake_minimum_required(VERSION 2.6.4)

set(PROJECT_NAME_STR cassandra)
set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")

set(CASS_DRIVER_PROJECT_NAME ${PROJECT_NAME_STR})
project(${PROJECT_NAME_STR} C CXX)

#-------------------
# Version
#-------------------
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 7)
set(PROJECT_VERSION_PATCH 0)
set(PROJECT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

# TODO(mstump) add version header

#-------------------
# Dependencies
#-------------------

# libuv
set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR} ENV LIBUV_ROOT_DIR)

if(WIN32)
  set(_LIBUV_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/libuv/")
  set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS ${_LIBUV_ROOT_HINTS} PATHS ${_LIBUV_ROOT_PATHS})
else()
  set(_LIBUV_ROOT_PATHS "/usr/" "/usr/local/")
  set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS ${_LIBUV_ROOT_HINTS} PATHS ${_LIBUV_ROOT_PATHS})
endif()

find_path(LIBUV_INCLUDE_DIR
  NAMES uv.h 
  HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)

find_library(LIBUV_LIBRARY
  NAMES uv libuv
  HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)

set(LIBUV_LIBRARIES ${LIBUV_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
  LIBUV_LIBRARIES
  LIBUV_INCLUDE_DIR)

set(INCLUDES ${INCLUDES} ${LIBUV_INCLUDE_DIR})
set(LIBS ${LIBS} ${LIBUV_LIBRARIES})

# openssl
if(WIN32)
  set(_OPENSSL_VERSION "1.0.1h")
  set(OPENSSL_ROOT_DIR "${PROJECT_SOURCE_DIR}/lib/openssl/")
  #set(ZLIB_NAMES zlibwapi)
  #set(ZLIB_ROOT "${PROJECT_SOURCE_DIR}/lib/zlib/")
endif()

find_package(OpenSSL REQUIRED)
set(LIBS ${LIBS} ${OPENSSL_LIBRARIES})
set(INCLUDES ${INCLUDES} ${OPENSSL_INCLUDE_DIR} )

#find_package(ZLIB REQUIRED)
#set(LIBS ${LIBS} ${ZLIB_LIBRARIES})
#set(INCLUDES ${INCLUDES} ${ZLIB_INCLUDE_DIR} )

if(WIN32)
  set(LIBS ${LIBS} iphlpapi psapi wsock32 ws2_32)
endif()

# Build up the include path's
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})
set(INCLUDES ${INCLUDES} "${PROJECT_SOURCE_DIR}/src/third_party/boost")

#-------------------
# Compiler flags
#-------------------

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

if(MSVC)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MD")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MDd")
  set(CMAKE_SHARED_LINKER_FLAGS_DEBUG "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:LIBCMT")
 
  # On Visual C++ -pedantic flag is not used,
  # -fPIC is not used on Windows platform (all DLLs are
  # relocable), -Wall generates about 30k stupid warnings
  # that can hide useful ones.

  # TODO(mpenick): Fix these "possible loss of data" warnings
  set(PLATFORM_DEPENDENT_CXX_FLAGS "-D_CRT_SECURE_NO_WARNINGS /wd4244 /wd4267")

  # needed for windows sockets (not verified)
  add_definitions(-D_WIN32_WINNT=0x0501)
else()
  # GCC/Clang specific compiler options
  # I disabled long-long warning because boost generates about 50 such warnings
  set(PLATFORM_DEPENDENT_CXX_FLAGS "-std=c++98 -fPIC -Wall -pedantic -Wextra -Werror -Wno-long-long -Wno-deprecated-declarations -Wno-unused-parameter")
endif()

#-------------------
# Cassandra static and shared
#-------------------
include_directories(${INCLUDES})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# we must add header files as dependencies (if header
# changes project must be recompiled, right).
file(GLOB INC_FILES
  ${PROJECT_SOURCE_DIR}/include/*.h )

file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp) 

set(SRC_FILES ${SRC_FILES}
  "${PROJECT_SOURCE_DIR}/src/third_party/boost/libs/atomic/src/lockpool.cpp" 
  "${PROJECT_SOURCE_DIR}/src/third_party/boost/libs/random/src/random_device.cpp")

source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${INC_FILES})

set(ALL_SOURCE_FILES ${SRC_FILES} ${INC_FILES})

# build dynamic and static version of library
add_library(${PROJECT_LIB_NAME} SHARED ${ALL_SOURCE_FILES})
add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${ALL_SOURCE_FILES})

target_link_libraries(${PROJECT_LIB_NAME} ${LIBS})
target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${LIBS})

set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})

set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})

set(PROJECT_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

set_property(
  TARGET ${PROJECT_LIB_NAME}
  APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} ${PLATFORM_DEPENDENT_CXX_FLAGS} -DCASS_BUILDING")

set_property(
  TARGET ${PROJECT_LIB_NAME_STATIC}
  APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} ${PLATFORM_DEPENDENT_CXX_FLAGS} -DCASS_STATIC")

#-------------------
# Install target
#-------------------

# Where to put headers
set(INSTALL_HEADERS_DIR "${CMAKE_INSTALL_PREFIX}/include/")
install(FILES ${INC_FILES} DESTINATION "${INSTALL_HEADERS_DIR}")

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
# Uninstall target
#-------------------

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(UNINSTALL
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

#-------------------
# Unit tests
#-------------------

add_subdirectory(test/unit_tests)

#-------------------
# Integration tests
#-------------------

if(WIN32)
  set(BOOST_ROOT "${PROJECT_SOURCE_DIR}/lib/boost/")
  set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/")
  set(INTEGRATION_TEST_CXX_FLAGS "${CMAKE_C_FLAGS} -D_CRT_SECURE_NO_WARNINGS")
else()
  set(INTEGRATION_TEST_CXX_FLAGS "-std=c++0x -fPIC -Wall -pedantic -Wextra -Werror -Wno-long-long -Wno-deprecated-declarations -Wno-unused-parameter")
endif()

find_package(Boost 1.41.0 COMPONENTS system thread unit_test_framework date_time program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/test/ccm_bridge/cmake/Modules/")
find_package(LIBSSH2)

if((Boost_FOUND) AND (LIBSSH2_FOUND))
  add_subdirectory(test/ccm_bridge)
  add_subdirectory(test/integration_tests)
endif()

#-------------------
# Examples
#-------------------

if(MSVC)
  set(EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} -D_CRT_SECURE_NO_WARNINGS")
else()
  set(EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} -std=c89 -fPIC -Wall -pedantic -Wextra -Werror -Wno-long-long -Wno-deprecated-declarations -Wno-unused-parameter")
endif()

add_subdirectory(examples/async)
add_subdirectory(examples/basic)
add_subdirectory(examples/collections)
add_subdirectory(examples/prepared)
add_subdirectory(examples/batch)
add_subdirectory(examples/uuids)
add_subdirectory(examples/simple)

#-----------------------------------
# Generating API docs with Doxygen
#-----------------------------------

find_package(Doxygen)

if(DOXYGEN_FOUND)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  add_custom_target(doc
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)

