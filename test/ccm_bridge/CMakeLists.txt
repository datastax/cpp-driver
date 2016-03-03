cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

# Force OLD style of leaving VERSION variables untouched
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 OLD)
endif()

# Title the project
set(PROJECT_CCM_BRIDGE_LIB_NAME CCMBridge)
project(${PROJECT_CCM_BRIDGE_LIB_NAME} C CXX)

# Options
option(CCM_VERBOSE_LOGGING "Disable/Enable CCM verbose console logging" OFF)
if(CCM_VERBOSE_LOGGING)
  add_definitions(-DCCM_VERBOSE_LOGGING)
endif()
option(OPENSSL_CLEANUP "Cleanup memory resources used by OpenSSL" ON)
if(OPENSSL_CLEANUP)
  include_directories(${OPENSSL_INCLUDE_DIR})
  add_definitions(-DOPENSSL_CLEANUP)
endif()

# Constants
set(CCM_BRIDGE_CONFIGURATION_FILE ${PROJECT_SOURCE_DIR}/data/config.txt) # CCM configuration file

# Gather the header and source files
file(GLOB CCM_BRIDGE_INC_FILES ${PROJECT_SOURCE_DIR}/src/*.hpp)
file(GLOB CCM_BRIDGE_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

# Build up the include paths
set(INTEGRATION_TESTS_INCLUDES ${PROJECT_SOURCE_DIR}/src ${Boost_INCLUDE_DIRS})
if(LIBSSH2_FOUND)
  set(INTEGRATION_TESTS_INCLUDES ${INTEGRATION_TESTS_INCLUDES} ${LIBSSH2_INCLUDE_DIRS})
endif()

# Assign the include directories
include_directories(${INTEGRATION_TESTS_INCLUDES})

# Build up the libraries paths
set(INTEGRATION_TESTS_LIBS ${Boost_LIBRARIES} ${OPENSSL_LIBRARIES})
if(LIBSSH2_FOUND)
  set(INTEGRATION_TEST_LIBS ${INTEGRATION_TEST_LIBS} ${LIBSSH2_LIBRARIES})
endif()
if(ZLIB_FOUND)
  set(INTEGRATION_TESTS_LIBS ${INTEGRATION_TESTS_LIBS} ${ZLIB_LIBRARIES})
endif()

# Set platform conditional compilation flags
# Define WIN32 on windows, and UNIX on Linux/OS X/Unix
if(WIN32)
  # Assign additional library requirements for Windows (winsock2)
  set(INTEGRATION_TESTS_LIBS ${INTEGRATION_TESTS_LIBS} wsock32 ws2_32)

  # Add preprocessor definition for Windows
  add_definitions(-DWIN32)
elseif(UNIX)
  # Add preprocessor definition for *nix
  add_definitions(-DUNIX)
endif()

# Create header and source groups (mainly for Visual Studio generator)
source_group("Source Files" FILES ${CCM_BRIDGE_SRC_FILES})
source_group("Header Files" FILES ${CCM_BRIDGE_INC_FILES})

# Build the CCM bridge static library
add_library(CCMBridge STATIC ${CCM_BRIDGE_SRC_FILES} ${CCM_BRIDGE_INC_FILES} ${CCM_BRIDGE_CONFIGURATION_FILE})
target_link_libraries(CCMBridge ${INTEGRATION_TESTS_LIBS})

# Copy data file to output directory
configure_file(${CCM_BRIDGE_CONFIGURATION_FILE} ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
