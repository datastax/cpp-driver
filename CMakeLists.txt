cmake_minimum_required(VERSION 2.6.4)

# Ensure functions/modules are available
include(FindPackageHandleStandardArgs)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

set(PROJECT_NAME_STR cassandra)
set(PROJECT_LIB_NAME ${PROJECT_NAME_STR})
set(PROJECT_LIB_NAME_STATIC "${PROJECT_LIB_NAME}_static")

# TODO: Figure out Mac OS X rpath
if(POLICY CMP0042)
  cmake_policy(SET CMP0042 OLD)
endif()

# Title the project and define the versioning
project(${PROJECT_NAME_STR} C CXX)
set(PROJECT_VERSION_MAJOR 2)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH 1)
set(PROJECT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

set(CASS_DRIVER_PROJECT_NAME ${PROJECT_NAME_STR})

# Assign specialized 32-bit compiler flags
if(32BIT)
  set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS FALSE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m32")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -m32")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -m32")
endif()

# Shorten the source file pathing for log messages
# TODO: Investigate if NMake works correctly on Windows (need to be able to detect generator used)
if(NOT WIN32)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLOG_FILE_='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"'")
endif()

# Dependency minimum version
set(CASS_MINIMUM_BOOST_VERSION 1.55.0)

# Constants
set(MULTICORE_CORES_NUMBER "3" CACHE STRING "Number of cores (for multicore compilation)")

# Options
option(CASS_BUILD_SHARED "Build shared library" ON)
option(CASS_BUILD_STATIC "Build static library" OFF)
option(CASS_BUILD_EXAMPLES "Build examples" OFF)
option(CASS_BUILD_DOCS "Build documentation" OFF)
option(CASS_BUILD_TESTS "Build tests" OFF)
option(CASS_INSTALL_HEADER "Install header file" ON)
option(CASS_MULTICORE_COMPILATION "Enable multicore compilation" OFF)
option(CASS_USE_STATIC_LIBS "Link static libraries when building executables" OFF)
option(CASS_USE_BOOST_ATOMIC "Use Boost atomics library" OFF)
option(CASS_USE_STD_ATOMIC "Use C++11 atomics library" OFF)
option(CASS_USE_OPENSSL "Use OpenSSL" ON)
option(CASS_USE_TCMALLOC "Use tcmalloc" OFF)
option(CASS_USE_ZLIB "Use zlib" OFF)

if(CASS_BUILD_TESTS)
  set(CASS_BUILD_STATIC ON) # Required for unit tests
endif()

# Determine which driver target should be used as a dependency
set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME})
if(CASS_USE_STATIC_LIBS)
  set(CASS_BUILD_STATIC ON)
  set(PROJECT_LIB_NAME_TARGET ${PROJECT_LIB_NAME_STATIC})
endif()

# Ensure the driver is configured to build
if(NOT CASS_BUILD_SHARED AND NOT CASS_BUILD_STATIC)
  message(FATAL_ERROR "Driver is not Configured to Build: Ensure shared and/or static library is enabled")
endif()

# Ensure Windows platform is supported
if(WIN32)
  if(CMAKE_SYSTEM_VERSION GREATER 5.2 OR
     CMAKE_SYSTEM_VERSION EQUAL 5.2)
    add_definitions(-D_WIN32_WINNT=0x502)
  else()
    string(REPLACE "." "" WINDOWS_VERSION ${CMAKE_SYSTEM_VERSION})
    string(REGEX REPLACE "([0-9])" "0\\1" WINDOWS_VERSION ${WINDOWS_VERSION})
    message(FATAL_ERROR "Unable to build driver: Unsupported Windows platform 0x${WINDOWS_VERSION}")
  endif()
endif()

# Build up the include paths
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(CASS_INCLUDES ${CASS_INCLUDES} ${PROJECT_INCLUDE_DIR})
set(CASS_INCLUDES ${CASS_INCLUDES} ${PROJECT_SOURCE_DIR}/src)
set(CASS_INCLUDES ${CASS_INCLUDES} ${PROJECT_SOURCE_DIR}/src/third_party/rapidjson)

#---------------
# Dependencies
#---------------

# libuv
# TODO: Create a CMake module for libuv detection
# Setup the paths and hints for libuv
set(_LIBUV_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/libuv/")
set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR} $ENV{LIBUV_ROOT_DIR})
if(NOT WIN32)
  set(_LIBUV_ROOT_PATHS "${_LIBUV_ROOT_PATHS}" "/usr/" "/usr/local/")
endif()
set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS
  HINTS ${_LIBUV_ROOT_HINTS}
  PATHS ${_LIBUV_ROOT_PATHS})

# Ensure libuv was found
find_path(LIBUV_INCLUDE_DIR
  NAMES uv.h
  HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)
find_library(LIBUV_LIBRARY
  NAMES uv libuv
  HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
  LIBUV_LIBRARY
  LIBUV_INCLUDE_DIR)

# Assign libuv include and libraries
set(CASS_INCLUDES ${CASS_INCLUDES} ${LIBUV_INCLUDE_DIR})
set(CASS_LIBS ${CASS_LIBS} ${LIBUV_LIBRARY})

#------------------------
# Optional Dependencies
#------------------------

# Boost
if(CASS_USE_BOOST_ATOMIC OR CASS_BUILD_TESTS)
  # Allow for boost directory to be specified on the command line
  set(ENV{BOOST_ROOT} "${PROJECT_SOURCE_DIR}/lib/boost/")
  if(BOOST_ROOT_DIR)
    set(ENV{BOOST_ROOT} ${BOOST_ROOT_DIR})
  endif()

  # Ensure Boost auto linking is disabled (defaults to auto linking on Windows)
  if(WIN32)
    add_definitions(-DBOOST_ALL_NO_LIB)
  endif()

  # Determine if shared or static boost libraries should be used
  if(CASS_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS ON)
  else()
    set(Boost_USE_STATIC_LIBS OFF)
    add_definitions(-DBOOST_ALL_DYN_LINK)
  endif()
  set(Boost_USE_STATIC_RUNTIME OFF)
  set(Boost_USE_MULTITHREADED ON)
  add_definitions(-DBOOST_THREAD_USES_MOVE)

  # Ensure the driver components exist (optional)
  if(CASS_USE_BOOST_ATOMIC)
    find_package(Boost)
    if(NOT Boost_INCLUDE_DIR)
      message(FATAL_ERROR "Boost headers required to build driver because of -DCASS_USE_BOOST_ATOMIC=On")
    endif()

    # Assign Boost include and libraries
    set(CASS_INCLUDES ${CASS_INCLUDES} ${Boost_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${Boost_LIBRARIES})
  endif()

  # Ensure unit and integration test boost components exist
  if(CASS_BUILD_TESTS)
    find_package(Boost ${CASS_MINIMUM_BOOST_VERSION} COMPONENTS chrono date_time filesystem log log_setup system regex thread unit_test_framework)
    if(NOT Boost_FOUND)
      message(FATAL_ERROR "Boost [chrono, date_time, filesystem, log, log_setup, system, regex, thread, and unit_test_framework] are required to build tests")
    endif()

    # Assign Boost include and libraries
    set(CASS_INCLUDES ${CASS_INCLUDES} ${Boost_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${Boost_LIBRARIES})
  endif()

  # Determine if additional Boost definitions are required for driver/executables
  set(CASS_FOUND_BOOST_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  if((CASS_FOUND_BOOST_VERSION VERSION_GREATER "1.56.0") OR (CASS_FOUND_BOOST_VERSION VERSION_EQUAL "1.56.0"))
    if(NOT WIN32)
      # Handle explicit initialization warning in atomic/details/casts
      add_definitions(-Wno-missing-field-initializers)
    endif()
  else()
    # Handle redefinition warning of BOOST_NO_CXX11_RVALUE_REFERENCES
    add_definitions(-DBOOST_NO_CXX11_RVALUE_REFERENCES)
  endif()
endif()

# gperftools (tcmalloc)
# TODO: Create a CMake module for gperftools detection
if(CASS_USE_TCMALLOC)
  # Setup the paths and hints for gperftools
  set(_GPERFTOOLS_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/gperftools/")
  set(_GPERFTOOLS_ROOT_HINTS ${GPERFTOOLS_ROOT_DIR} $ENV{GPERFTOOLS_ROOT_DIR})
  if(NOT WIN32)
    set(_GPERFTOOLS_ROOT_PATHS ${_GPERFTOOLS_ROOT_PATHS} "/usr/" "/usr/local/")
  endif()
  set(_GPERFTOOLS_ROOT_HINTS_AND_PATHS
    HINTS ${_GPERFTOOLS_ROOT_HINTS}
    PATHS ${_GPERFTOOLS_ROOT_PATHS})

  # Ensure gperftools (tcmalloc) was found
  find_library(GPERFLIBRARY_LIBRARY
    NAMES tcmalloc
    HINTS ${_GPERFTOOLS_LIBDIR} ${_GPERFTOOLS_ROOT_HINTS_AND_PATHS}
    PATH_SUFFIXES lib)
  find_package_handle_standard_args(Gperftools "Could NOT find gperftools, try to set the path to the gperftools root folder in the system variable GPERFTOOLS_ROOT_DIR"
    GPERFLIBRARY_LIBRARY)

  # Assign gperftools (tcmalloc) library
  set(CASS_LIBS ${CASS_LIBS} ${GPERFLIBRARY_LIBRARY})
endif()

# zlib
if(CASS_USE_ZLIB)
  # Setup the root directory for zlib
  set(ZLIB_ROOT "${PROJECT_SOURCE_DIR}/lib/zlib/")
  set(ZLIB_ROOT ${ZLIB_ROOT} ${ZLIB_ROOT_DIR} $ENV{ZLIB_ROOT_DIR})

  # Ensure zlib was found (assign zlib include/libraries or present warning)
  find_package(ZLIB)
  if(ZLIB_FOUND)
    # Determine if the static library needs to be used for Windows
    if(WIN32 AND CASS_USE_STATIC_LIBS)
      string(REPLACE "zlib.lib" "zlibstatic.lib" ZLIB_LIBRARIES "${ZLIB_LIBRARIES}")
    endif()

    # Assign zlib properties
    set(CASS_INCLUDES ${CASS_INCLUDES} ${ZLIB_INCLUDE_DIRS})
    set(CASS_LIBS ${CASS_LIBS} ${ZLIB_LIBRARIES})
  else()
    message(WARNING "Could not find zlib, try to set the path to zlib root folder in the system variable ZLIB_ROOT_DIR")
    message(WARNING "zlib libraries will not be linked into build")
  endif()
endif()

# OpenSSL
if(CASS_USE_OPENSSL)
  # Setup the paths and hints for OpenSSL
  set(_OPENSSL_ROOT_PATHS "${PROJECT_SOURCE_DIR}/lib/openssl/")
  set(_OPENSSL_ROOT_HINTS ${OPENSSL_ROOT_DIR} $ENV{OPENSSL_ROOT_DIR})
  set(_OPENSSL_ROOT_HINTS_AND_PATHS
    HINTS ${_OPENSSL_ROOT_HINTS}
  PATHS ${_OPENSSL_ROOT_PATHS})

  # Discover OpenSSL and assign OpenSSL include and libraries
  find_package(OpenSSL REQUIRED)
  set(CASS_INCLUDES ${CASS_INCLUDES} ${OPENSSL_INCLUDE_DIR})
  set(CASS_LIBS ${CASS_LIBS} ${OPENSSL_LIBRARIES})
endif()

#--------------------
# Test Dependencies
#--------------------

# libssh2
if(CASS_BUILD_TESTS)
  # Setup the root directory for libssh2
  set(LIBSSH2_ROOT "${PROJECT_SOURCE_DIR}/lib/libssh2/" $ENV{LIBSSH2_ROOT})
  set(LIBSSH2_ROOT ${LIBSSH2_ROOT} ${LIBSSH2_ROOT_DIR} $ENV{LIBSSH2_ROOT_DIR})

  # Discover libssh2
  find_package(LIBSSH2 REQUIRED)
  if(NOT LIBSSH2_FOUND)
    message(FATAL_ERROR "libssh2 are required to build tests")
  endif()

  # Assign test libraries (additional boost and libssh2 dependencies)
  set(CASS_TEST_LIBS ${Boost_LIBRARIES} ${LIBSSH2_LIBRARIES})
  if(UNIX)
    set(CASS_TEST_LIBS ${CASS_TEST_LIBS} pthread)
  endif()
endif()

# Assign additional library requirements for Windows
if(WIN32)
  set(CASS_LIBS ${CASS_LIBS} iphlpapi psapi wsock32 ws2_32)
endif()

#----------------------
# Generating API docs
#----------------------

# Doxygen
if(CASS_BUILD_DOCS)
  find_package(Doxygen)
  if(DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(docs
      ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generating API documentation with Doxygen" VERBATIM)
  endif()
endif()

#-------------------
# Compiler Flags
#-------------------

# Determine if all GNU extensions should be enabled
if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR
   "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # CMAKE_CXX_COMPILER variables do not exist in 2.6.4 (min version)
  # Parse the -dumpversion argument into the variable not already set
  if("${CMAKE_CXX_COMPILER_VERSION}" STREQUAL "")
    execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION)
  endif()
endif()

# Determine if std::atomic can be used for GCC, Clang, or MSVC
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
    # Version determined from: https://gcc.gnu.org/wiki/Atomic/GCCMM
    if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "4.7" OR
       CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "4.7")
      set(CASS_USE_STD_ATOMIC ON)
    endif()
  endif()
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # Version determined from: http://clang.llvm.org/cxx_status.html
  # 3.2 includes the full C++11 memory model, but 3.1 had atomic
  # support.
  if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "3.1" OR
     CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "3.1")
   set(CASS_USE_STD_ATOMIC ON)
  endif()
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  # Version determined from https://msdn.microsoft.com/en-us/library/hh874894
  # VS2012+/VS 11.0+/WindowsSDK v8.0+
  if(MSVC_VERSION GREATER 1700 OR
     MSVC_VERSION EQUAL 1700)
    set(CASS_USE_STD_ATOMIC ON)
  endif()
endif()

# Enable specific cass::Atomic implementation
if(CASS_USE_BOOST_ATOMIC)
  message(STATUS "Using boost::atomic implementation for atomic operations")
  add_definitions(-DCASS_USE_BOOST_ATOMIC)
elseif(CASS_USE_STD_ATOMIC)
  message(STATUS "Using std::atomic implementation for atomic operations")
  add_definitions(-DCASS_USE_STD_ATOMIC)
endif()

# Assign compiler specific flags
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  # Determine if multicore compilation should be enabled and set
  if(CASS_MULTICORE_COMPILATION)
    add_definitions("/MP${MULTICORE_CORES_NUMBER}")
  endif()

  # Remove/Replace linker flags in the event they are present
  string(REPLACE "/INCREMENTAL" "/INCREMENTAL:NO" CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG}")
  string(REPLACE "/INCREMENTAL" "/INCREMENTAL:NO" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

  # Create specific linker flags
  if(CASS_USE_STATIC_LIBS)
    set(WINDOWS_LINKER_FLAGS "/INCREMENTAL:NO /LTCG /NODEFAULTLIB:LIBCMT.LIB /NODEFAULTLIB:LIBCMTD.LIB")
    set(PROJECT_CXX_LINKER_FLAGS "${WINDOWS_LINKER_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${WINDOWS_LINKER_FLAGS}")
  endif()

  # On Visual C++ -pedantic flag is not used,
  # -fPIC is not used on Windows platform (all DLLs are
  # relocable), -Wall generates about 30k stupid warnings
  # that can hide useful ones.
  # Create specific warning disable compiler flags
  # TODO(mpenick): Fix these "possible loss of data" warnings
  add_definitions(/wd4244)
  add_definitions(/wd4267)

  # Add preprocessor definitions for proper compilation
  add_definitions(-D_WIN32_WINNT=0x0501)      # Required for winsock (pre Windows XP wspiapi.h only)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)  # Remove warnings for not using safe functions (TODO: Fix codebase to be more secure for Visual Studio)
  add_definitions(-DNOMINMAX)                 # Does not define min/max macros

  # Create the project, example, and test flags
  set(PROJECT_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PROJECT_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
  set(EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
  set(TEST_CXX_FLAGS "${PROJECT_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  # GCC specific compiler options
  # I disabled long-long warning because boost generates about 50 such warnings
  set(WARNING_COMPILER_FLAGS "-Wall -pedantic -Wextra -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros")

  if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "4.8" OR
     CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "4.8")
    set(WARNING_COMPILER_FLAGS "${WARNING_COMPILER_FLAGS} -Wno-unused-local-typedefs")
  endif()

  # OpenSSL is deprecated on later versions of Mac OS X. The long-term solution
  # is to provide a CommonCryto implementation.
  if (APPLE AND CASS_USE_OPENSSL)
    set(PROJECT_CXX_FLAGS "${PROJECT_CXX_FLAGS} -Wno-deprecated-declarations")
  endif()

  # Enable C++11 support to use std::atomic
  if(CASS_USE_STD_ATOMIC)
    set(PROJECT_CXX_FLAGS "${PROJECT_CXX_FLAGS} -std=c++11")
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} -std=c++11")
  endif()

  set(PROJECT_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PROJECT_CXX_FLAGS} ${WARNING_COMPILER_FLAGS} -Werror")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "4.3" OR
     CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "4.3")
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
  else()
    # TODO(mpenick): stress.cpp might not compile on older compilers because of boost::unique_future<>
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
  endif()
  set(EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} ${WARNING_COMPILER_FLAGS}")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # Clang/Intel specific compiler options
  # I disabled long-long warning because boost generates about 50 such warnings
  set(WARNING_COMPILER_FLAGS "-Wall -pedantic -Wextra -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros -Wno-zero-length-array")

  # OpenSSL is deprecated on later versions of Mac OS X. The long-term solution
  # is to provide a CommonCryto implementation.
  if (APPLE AND CASS_USE_OPENSSL)
    set(PROJECT_CXX_FLAGS "${PROJECT_CXX_FLAGS} -Wno-deprecated-declarations")
  endif()

  # Enable C++11 support to use std::atomic
  if(CASS_USE_STD_ATOMIC)
    set(PROJECT_CXX_FLAGS "${PROJECT_CXX_FLAGS} -std=c++11")
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} -std=c++11")
  endif()

  set(PROJECT_CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${PROJECT_CXX_FLAGS} ${WARNING_COMPILER_FLAGS} -Werror")
  set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} ${WARNING_COMPILER_FLAGS}")
  set(EXAMPLE_C_FLAGS "${CMAKE_C_FLAGS} -std=c89 ${WARNING_COMPILER_FLAGS}")
else()
  message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

#------------------------------
# Cassandra static and shared
#------------------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Gather the header and source files
file(GLOB API_HEADER_FILES ${PROJECT_SOURCE_DIR}/include/*.h)
file(GLOB INC_FILES ${PROJECT_SOURCE_DIR}/src/*.hpp)
file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

# Determine atomic library to include
if(CASS_USE_BOOST_ATOMIC)
  set(INC_FILES ${INC_FILES}
    ${PROJECT_SOURCE_DIR}/src/atomic/atomic_boost.hpp)
elseif(CASS_USE_STD_ATOMIC)
  set(INC_FILES ${INC_FILES}
    ${PROJECT_SOURCE_DIR}/src/atomic/atomic_std.hpp)
else()
  set(INC_FILES ${INC_FILES}
    ${PROJECT_SOURCE_DIR}/src/atomic/atomic_intrinsics.hpp)
  if(WIN32)
    set(INC_FILES ${INC_FILES}
      ${PROJECT_SOURCE_DIR}/src/atomic/atomic_intrinsics_msvc.hpp)
  else()
    set(INC_FILES ${INC_FILES}
      ${PROJECT_SOURCE_DIR}/src/atomic/atomic_intrinsics_gcc.hpp)
  endif()
endif()

set(SRC_FILES ${SRC_FILES}
  ${PROJECT_SOURCE_DIR}/src/third_party/hdr_histogram/hdr_histogram.cpp)

# Determine if OpenSSL should be compiled in (or not)
if(CASS_USE_OPENSSL)
  set(INC_FILES ${INC_FILES}
    ${PROJECT_SOURCE_DIR}/src/ssl/ssl_openssl_impl.hpp
    ${PROJECT_SOURCE_DIR}/src/ssl/ring_buffer_bio.hpp)
  set(SRC_FILES ${SRC_FILES}
    ${PROJECT_SOURCE_DIR}/src/ssl/ssl_openssl_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/ssl/ring_buffer_bio.cpp)
  add_definitions(-DCASS_USE_OPENSSL)
else()
  set(INC_FILES ${INC_FILES}
    ${PROJECT_SOURCE_DIR}/src/ssl/ssl_no_impl.hpp)
  set(SRC_FILES ${SRC_FILES}
    ${PROJECT_SOURCE_DIR}/src/ssl/ssl_no_impl.cpp)
endif()

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${API_HEADER_FILES} ${INC_FILES})
set(ALL_SOURCE_FILES ${SRC_FILES} ${API_HEADER_FILES} ${INC_FILES})

# Create an object library for the driver (single build)
if(NOT CMAKE_VERSION VERSION_LESS "2.8.8")
  add_library(cpp-driver OBJECT ${ALL_SOURCE_FILES})
  if(NOT WIN32)
    set_property(TARGET cpp-driver PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -fPIC")
  endif()

  # Build both shared and static libraries
  set(CASS_BUILD_SHARED ON)
  set(CASS_BUILD_STATIC ON)
endif()

# Assign the include directories
include_directories(${CASS_INCLUDES})

# Determine if the dynamic/shared library should be built
if(CASS_BUILD_SHARED)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME} SHARED ${ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME} SHARED $<TARGET_OBJECTS:cpp-driver>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME} ${CASS_LIBS})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_target_properties(${PROJECT_LIB_NAME} PROPERTIES LINK_FLAGS "${PROJECT_CXX_LINKER_FLAGS}")
  set_property(
    TARGET ${PROJECT_LIB_NAME}
    APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -DCASS_BUILDING")
endif()

# Determine if the static library should be built
if(CASS_BUILD_STATIC)
  if(CMAKE_VERSION VERSION_LESS "2.8.8")
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC ${ALL_SOURCE_FILES})
  else()
    add_library(${PROJECT_LIB_NAME_STATIC} STATIC $<TARGET_OBJECTS:cpp-driver>)
  endif()
  target_link_libraries(${PROJECT_LIB_NAME_STATIC} ${LIBS})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES OUTPUT_NAME ${PROJECT_LIB_NAME_STATIC})
  set_target_properties(${PROJECT_LIB_NAME_STATIC} PROPERTIES VERSION ${PROJECT_VERSION_STRING} SOVERSION ${PROJECT_VERSION_MAJOR})
  set_property(
    TARGET ${PROJECT_LIB_NAME_STATIC}
    APPEND PROPERTY COMPILE_FLAGS "${PROJECT_CXX_FLAGS} -DCASS_STATIC")

  # Update the test flags to indicate the use of the static library
  if(CASS_USE_STATIC_LIBS)
    set(TEST_CXX_FLAGS "${TEST_CXX_FLAGS} -DCASS_STATIC")
  endif()
endif()

#-------------------------------------
# Directory installation information
#-------------------------------------

# Determine if the library directory needs to be determined
if(NOT DEFINED CMAKE_INSTALL_LIBDIR)
  # Properly assign lib or lib64 directory based on determined architecture
  get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
  if("${LIB64}" STREQUAL "TRUE")
      set(LIBSUFFIX 64)
  else()
      set(LIBSUFFIX "")
  endif()

  # Assign the library directory
  set(CMAKE_INSTALL_LIBDIR "lib${LIBSUFFIX}")
endif()

# Create a binary directory executable and DLLs (windows only)
set(INSTALL_DLL_EXE_DIR "bin")

#-----------------------------
# Unit and integration tests
#-----------------------------

# Add the unit and integration tests to the build process
if(CASS_BUILD_TESTS)
  # Add the unit test project
  add_subdirectory(test/unit_tests)

  # Add CCM bridge as a dependency for integration tests
  add_subdirectory("${PROJECT_SOURCE_DIR}/test/ccm_bridge")
  set(CCM_BRIDGE_INCLUDES "${PROJECT_SOURCE_DIR}/test/ccm_bridge/include")

  # Add the integration test project
  add_subdirectory(test/integration_tests)
endif()

#-----------
# Examples
#-----------

if(CASS_BUILD_EXAMPLES)
  add_subdirectory(examples/async)
  add_subdirectory(examples/basic)
  add_subdirectory(examples/batch)
  add_subdirectory(examples/bind_by_name)
  add_subdirectory(examples/callbacks)
  add_subdirectory(examples/collections)
  add_subdirectory(examples/logging)
  add_subdirectory(examples/maps)
  add_subdirectory(examples/prepared)
  add_subdirectory(examples/uuids)
  add_subdirectory(examples/schema_meta)
  add_subdirectory(examples/simple)
  add_subdirectory(examples/ssl)
  add_subdirectory(examples/paging)
  add_subdirectory(examples/perf)
endif()

#------------------
# Install targets
#------------------

# Determine if the header should be installed
if(CASS_INSTALL_HEADER)
  install(FILES ${API_HEADER_FILES} DESTINATION "include")
endif()

# Install the dynamic/shared library
if(CASS_BUILD_SHARED)
  install(TARGETS ${PROJECT_LIB_NAME}
    RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
endif()

# Install the static library
if(CASS_BUILD_STATIC)
  install(TARGETS ${PROJECT_LIB_NAME_STATIC}
    RUNTIME DESTINATION ${INSTALL_DLL_EXE_DIR}  # for dll/executable files
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # for shared library
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}) # for static library
endif()

#-------------------
# Uninstall target
#-------------------

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(UNINSTALL
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

#-----------------------------------
# Build an RPM.
#-----------------------------------
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION_STRING})
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "cpp-cassandra-driver")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "C++ driver for Cassandra")
set(CPACK_PACKAGE_CONTACT "Michael Penick")
set(CPACK_PACKAGE_VENDOR "Datastax")
if(32BIT)
  set(CPACK_RPM_PACKAGE_ARCHITECTURE i686)
else()
  set(CPACK_RPM_PACKAGE_ARCHITECTURE x86_64)
endif()
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CPACK_RPM_PACKAGE_ARCHITECTURE}")
set(CPACK_RPM_PACKAGE_REQUIRES "libuv")
include(CPack)
