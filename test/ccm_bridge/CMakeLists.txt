cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 OLD)
endif()

# Title the project
set(PROJECT_CCM_BRIDGE_LIB_NAME CCMBridge)
project(${PROJECT_CCM_BRIDGE_LIB_NAME} C CXX)

# Constants
set(CCM_BRIDGE_CONFIGURATION_FILE ${PROJECT_SOURCE_DIR}/data/config.txt) # CCM configuration file

# Gather the header and source files
file(GLOB CCM_BRIDGE_INC_FILES ${PROJECT_SOURCE_DIR}/include/*.hpp)
file(GLOB CCM_BRIDGE_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

# Build up the include paths
set(INTEGRATION_TESTS_INCLUDES ${PROJECT_SOURCE_DIR}/include
  ${Boost_INCLUDE_DIRS}
  ${LIBSSH2_INCLUDE_DIRS})

# Assign the include directories
include_directories(${INTEGRATION_TESTS_INCLUDES})

# Build up the libraries paths
set(INTEGRATION_TESTS_LIBS ${Boost_LIBRARIES}
  ${LIBSSH2_LIBRARIES}
  ${OPENSSL_LIBRARIES})
if(ZLIB_FOUND)
  set(INTEGRATION_TESTS_LIBS ${INTEGRATION_TESTS_LIBS} ${ZLIB_LIBRARIES})
endif()

# Ensure the Boost logger can be utilized
if(NOT Boost_LOG_FOUND)
  add_definitions(-DCQL_NO_BOOST_LOG)
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

# Install the CCM bridge library
install(TARGETS ${PROJECT_CCM_BRIDGE_LIB_NAME} ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Copy data file to output directory
configure_file(${CCM_BRIDGE_CONFIGURATION_FILE} ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
