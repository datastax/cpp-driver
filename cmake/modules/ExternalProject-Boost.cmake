# Copyright (c) DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
include(ExternalProject)
include(Windows-Environment)
include(ProcessorCount)

# Boost related CMake options
option(BOOST_INSTALL_PREFIX "Boost installation prefix location")
if(NOT BOOST_INSTALL_PREFIX)
  set(BOOST_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/libs/boost" CACHE STRING "Boost installation prefix" FORCE)
endif()
option(BOOST_VERSION "Boost version to build and install")
if(NOT BOOST_VERSION)
  set(BOOST_VERSION "1.69.0")
endif()
set(BOOST_VERSION ${BOOST_VERSION} CACHE STRING "Boost version to build and install" FORCE)

# Handle the CMake version requirement for for modern Boost version(s)
if(BOOST_VERSION VERSION_EQUAL "1.66.0" OR BOOST_VERSION VERSION_GREATER "1.66.0")
  # Ensure CMake version is v3.11.0+
  if(CMAKE_VERSION VERSION_LESS 3.11.0)
    message(FATAL_ERROR "Boost v${BOOST_VERSION} requires CMake v3.11.0+.")
  endif()
endif()

# Break down the Boost version into its components
string(REPLACE "." ";" BOOST_VERSION_LIST ${BOOST_VERSION})
list(GET BOOST_VERSION_LIST 0 BOOST_VERSION_MAJOR)
list(GET BOOST_VERSION_LIST 1 BOOST_VERSION_MINOR)
list(GET BOOST_VERSION_LIST 2 BOOST_SUBMINOR_VERSION)

# Determine the Boost archive name to download
string(REPLACE "." "_" BOOST_ARCHIVE_VERSION ${BOOST_VERSION})
set(BOOST_ARCHIVE_NAME boost_${BOOST_ARCHIVE_VERSION} CACHE STRING "Boost archive name" FORCE)

# Boost external project variables
set(BOOST_LIBRARY_NAME "boost-library")
set(BOOST_PROJECT_PREFIX ${CMAKE_BINARY_DIR}/external/boost)
set(BOOST_ARCHIVE_URL_PREFIX "https://sourceforge.net/projects/boost/files/boost/${BOOST_VERSION}/")
set(BOOST_ARCHIVE_URL_SUFFIX ".tar.gz")
set(BOOST_ARCHIVE_URL "${BOOST_ARCHIVE_URL_PREFIX}${BOOST_ARCHIVE_NAME}${BOOST_ARCHIVE_URL_SUFFIX}")

# Make sure Visual Studio is available
if(NOT MSVC)
  message(FATAL_ERROR "Visual Studio is required to build Boost")
endif()
message(STATUS "Boost: v${BOOST_VERSION}")

# Boost library configuration variables
set(BOOST_INSTALL_DIR "${BOOST_INSTALL_PREFIX}" CACHE STRING "Boost installation directory" FORCE)
set(BOOST_BINARY_DIR "${BOOST_INSTALL_DIR}/bin" CACHE STRING "Boost binary directory" FORCE)
set(BOOST_INCLUDE_DIR "${BOOST_INSTALL_DIR}/include" CACHE STRING "Boost include directory" FORCE)
set(BOOST_LIBRARY_DIR "${BOOST_INSTALL_DIR}/lib${ARCH_TYPE}-msvc-${VS_TOOLSET_VERSION}" CACHE STRING "Boost library directory" FORCE)
set(BOOST_ROOT_DIR "${BOOST_INSTALL_DIR}" CACHE STRING "Boost root directory" FORCE)

# Create the debug/release libraries to add for Boost
set(BOOST_RELEASE_LIBRARY_SUFFIX "vc${VS_INTERNAL_VERSION}-mt-x${ARCH_TYPE}-${BOOST_VERSION_MAJOR}_${BOOST_VERSION_MINOR}.lib")
set(BOOST_RELEASE_LIBRARIES ${BOOST_LIBRARY_DIR}/libboost_atomic-${BOOST_RELEASE_LIBRARY_SUFFIX}
                            ${BOOST_LIBRARY_DIR}/libboost_chrono-${BOOST_RELEASE_LIBRARY_SUFFIX}
                            ${BOOST_LIBRARY_DIR}/libboost_system-${BOOST_RELEASE_LIBRARY_SUFFIX}
                            ${BOOST_LIBRARY_DIR}/libboost_thread-${BOOST_RELEASE_LIBRARY_SUFFIX}
                            ${BOOST_LIBRARY_DIR}/libboost_unit_test_framework-${BOOST_RELEASE_LIBRARY_SUFFIX}
                            CACHE STRING "Boost release libraries" FORCE)
set(BOOST_DEBUG_LIBRARY_SUFFIX "vc${VS_INTERNAL_VERSION}-mt-gd-x${ARCH_TYPE}-${BOOST_VERSION_MAJOR}_${BOOST_VERSION_MINOR}.lib")
set(BOOST_DEBUG_LIBRARIES ${BOOST_LIBRARY_DIR}/libboost_atomic-${BOOST_DEBUG_LIBRARY_SUFFIX}
                          ${BOOST_LIBRARY_DIR}/libboost_chrono-${BOOST_DEBUG_LIBRARY_SUFFIX}
                          ${BOOST_LIBRARY_DIR}/libboost_system-${BOOST_DEBUG_LIBRARY_SUFFIX}
                          ${BOOST_LIBRARY_DIR}/libboost_thread-${BOOST_DEBUG_LIBRARY_SUFFIX}
                          ${BOOST_LIBRARY_DIR}/libboost_unit_test_framework-${BOOST_DEBUG_LIBRARY_SUFFIX}
                          CACHE STRING "Boost debug libraries" FORCE)

# Aliases to stay consistent with FindBoost
set(BOOST_ROOT "${BOOST_INSTALL_DIR}" CACHE STRING "Boost root directory" FORCE)
set(BOOST_INCLUDEDIR "${BOOST_INCLUDE_DIR}" CACHE STRING "Boost include directory" FORCE)
set(Boost_INCLUDE_DIR "${BOOST_INCLUDE_DIR}" CACHE STRING "Boost include directory" FORCE)
set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_DIR}" CACHE STRING "Boost include directory" FORCE)
set(BOOST_LIBRARYDIR "${BOOST_LIBRARY_DIR}" CACHE STRING "Boost library directory" FORCE)
set(Boost_LIBRARY_DIRS "${BOOST_LIBRARY_DIR}" CACHE STRING "Boost library directory" FORCE)
set(Boost_LIBRARY_DIR_RELEASE "${BOOST_LIBRARY_DIR}" CACHE STRING "Boost release library directory" FORCE)
set(Boost_LIBRARY_DIR_DEBUG "${BOOST_LIBRARY_DIR}" CACHE STRING "Boost debug library directory" FORCE)
set(Boost_LIBRARIES "${BOOST_RELEASE_LIBRARIES}" CACHE STRING "Boost libraries" FORCE)
if(CMAKE_BUILD_TYPE MATCHES DEBUG)
  set(Boost_LIBRARIES "${BOOST_DEBUG_LIBRARIES}" CACHE STRING "Boost libraries" FORCE)
endif()
set(Boost_ATOMIC_LIBRARY_RELEASE ${BOOST_LIBRARY_DIR}/libboost_atomic-${BOOST_RELEASE_LIBRARY_SUFFIX})
set(Boost_CHRONO_LIBRARY_RELEASE ${BOOST_LIBRARY_DIR}/libboost_chrono-${BOOST_RELEASE_LIBRARY_SUFFIX})
set(Boost_SYSTEM_LIBRARY_RELEASE ${BOOST_LIBRARY_DIR}/libboost_system-${BOOST_RELEASE_LIBRARY_SUFFIX})
set(Boost_THREAD_LIBRARY_RELEASE ${BOOST_LIBRARY_DIR}/libboost_thread-${BOOST_RELEASE_LIBRARY_SUFFIX})
set(Boost_UNIT_TEST_FRAMEWORK_LIBRARY_RELEASE ${BOOST_LIBRARY_DIR}/libboost_unit_test_framework-${BOOST_RELEASE_LIBRARY_SUFFIX})
set(Boost_ATOMIC_LIBRARY_DEBUG ${BOOST_LIBRARY_DIR}/libboost_atomic-${BOOST_DEBUG_LIBRARY_SUFFIX})
set(Boost_CHRONO_LIBRARY_DEBUG ${BOOST_LIBRARY_DIR}/libboost_chrono-${BOOST_DEBUG_LIBRARY_SUFFIX})
set(Boost_SYSTEM_LIBRARY_DEBUG ${BOOST_LIBRARY_DIR}/libboost_system-${BOOST_DEBUG_LIBRARY_SUFFIX})
set(Boost_THREAD_LIBRARY_DEBUG ${BOOST_LIBRARY_DIR}/libboost_thread-${BOOST_DEBUG_LIBRARY_SUFFIX})
set(Boost_UNIT_TEST_FRAMEWORK_LIBRARY_DEBUG ${BOOST_LIBRARY_DIR}/libboost_unit_test_framework-${BOOST_DEBUG_LIBRARY_SUFFIX})

# Create a package name for the binaries
set(BOOST_PACKAGE_NAME "boost-${BOOST_VERSION}-${PACKAGE_ARCH_TYPE}-msvc${VS_INTERNAL_VERSION}.zip" CACHE STRING "Boost package name" FORCE)

# Create the configure script
file(TO_NATIVE_PATH ${BOOST_INSTALL_DIR} BOOST_NATIVE_INSTALL_DIR)
set(BOOST_CONFIGURE_SCRIPT "${BOOST_PROJECT_PREFIX}/scripts/configure_boost.bat")
file(REMOVE ${BOOST_CONFIGURE_SCRIPT})
file(WRITE ${BOOST_CONFIGURE_SCRIPT}
  "@REM Generated install script for Boost\r\n"
  "@ECHO OFF\r\n"
  "bootstrap.bat msvc --prefix=\"${BOOST_NATIVE_INSTALL_DIR}\"\r\n")
set(BOOST_CONFIGURE_COMMAND "${BOOST_CONFIGURE_SCRIPT}")

# Create the build/install script
ProcessorCount(BUILD_COMMAND_CORES)
set(BOOST_INCLUDE_VERSION_DIR "boost-${BOOST_VERSION_MAJOR}_${BOOST_VERSION_MINOR}")
file(TO_NATIVE_PATH ${BOOST_INCLUDE_DIR} BOOST_NATIVE_INCLUDE_DIR)
file(TO_NATIVE_PATH ${BOOST_INSTALL_DIR} BOOST_NATIVE_INSTALL_DIR)
file(TO_NATIVE_PATH ${BOOST_LIBRARY_DIR} BOOST_NATIVE_LIBRARY_DIR)
set(BOOST_BUILD_INSTALL_SCRIPT "${BOOST_PROJECT_PREFIX}/scripts/build_install_boost.bat")
file(REMOVE ${BOOST_BUILD_INSTALL_SCRIPT})
file(WRITE ${BOOST_BUILD_INSTALL_SCRIPT}
  "@REM Generated install script for Boost\r\n"
  "@ECHO OFF\r\n"
  "b2.exe install -j${BUILD_COMMAND_CORES} --prefix=\"${BOOST_NATIVE_INSTALL_DIR}\" --toolset=msvc-${VS_TOOLSET_VERSION} --with-atomic --with-chrono --with-system --with-thread --with-test address-model=${ARCH_TYPE} link=static threading=multi variant=debug,release\r\n"
  "IF EXIST \"${BOOST_NATIVE_INCLUDE_DIR}\\${BOOST_INCLUDE_VERSION_DIR}\\boost\" (\r\n"
  "  MOVE /Y \"${BOOST_NATIVE_INCLUDE_DIR}\\${BOOST_INCLUDE_VERSION_DIR}\\boost\" \"${BOOST_NATIVE_INCLUDE_DIR}\"\r\n"
  "  IF NOT %ERRORLEVEL% EQU 0 (\r\n"
  "    ENDLOCAL\r\n"
  "    EXIT /B 1\r\n"
  "  )\r\n"
  "  RMDIR /S/Q \"${BOOST_NATIVE_INCLUDE_DIR}\\${BOOST_INCLUDE_VERSION_DIR}\"\r\n"
  "  IF NOT %ERRORLEVEL% EQU 0 (\r\n"
  "    ENDLOCAL\r\n"
  "    EXIT /B 1\r\n"
  "  )\r\n"
  ")\r\n"
  "IF EXIST \"${BOOST_NATIVE_INSTALL_DIR}\\lib\" (\r\n"
  "  RENAME \"${BOOST_NATIVE_INSTALL_DIR}\\lib\" lib${ARCH_TYPE}-msvc-${VS_TOOLSET_VERSION}\r\n"
  "  IF NOT %ERRORLEVEL% EQU 0 (\r\n"
  "    ENDLOCAL\r\n"
  "    EXIT /B 1\r\n"
  "  )\r\n"
  ")\r\n")
set(BOOST_BUILD_INSTALL_COMMAND "${BOOST_BUILD_INSTALL_SCRIPT}")

# Add Boost as an external project
externalproject_add(${BOOST_LIBRARY_NAME}
  PREFIX ${BOOST_PROJECT_PREFIX}
  URL ${BOOST_ARCHIVE_URL}
  DOWNLOAD_DIR ${BOOST_PROJECT_PREFIX}
  CONFIGURE_COMMAND ${BOOST_CONFIGURE_COMMAND}
  BUILD_COMMAND ${BOOST_BUILD_INSTALL_COMMAND}
  INSTALL_COMMAND COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/LICENSE_1_0.txt ${BOOST_INSTALL_DIR}
  BUILD_IN_SOURCE 1
  LOG_DOWNLOAD 1
  LOG_CONFIGURE 1
  LOG_BUILD 1
  LOG_INSTALL 1)

# Update the include directory to use Boost
include_directories(${BOOST_INCLUDE_DIR})
