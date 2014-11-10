cmake_minimum_required(VERSION 2.6.4)

# Clear INCLUDE_DIRECTORIES to not include project-level includes
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

project(CCMBridge C CXX)

# Allow access to custom FindLIBSSH2 module
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

set(INCLUDES "")
set(LIBRARIES "")

set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})

if(Boost_LOG_FOUND AND Boost_LOG_SETUP_FOUND)
  set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})
  set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})
else()
  add_definitions(-DCQL_NO_BOOST_LOG)
endif()

# -------------------------------------------------------------

# Find libSSH2 library
# -------------------------------------------------------------
find_package(LIBSSH2 REQUIRED)

set(LIBRARIES ${LIBRARIES} ${LIBSSH2_LIBRARIES})
set(INCLUDES ${INCLUDES} ${LIBSSH2_INCLUDE_DIRS})

# -------------------------------------------------------------

# Find OpenSSL library
# -------------------------------------------------------------
find_package(OpenSSL REQUIRED)

set(LIBRARIES ${LIBRARIES} ${OPENSSL_LIBRARIES})
set(INCLUDES ${INCLUDES} ${OPENSSL_INCLUDE_DIR})
	
# -------------------------------------------------------------
# Find ZLIB library
# -------------------------------------------------------------
#find_package(ZLIB REQUIRED)
#set(LIBRARIES ${LIBRARIES} ${ZLIB_LIBRARIES})
#set(INCLUDES ${INCLUDES} ${ZLIB_INCLUDE_DIR})

# -------------------------------------------------------------

# Set platform conditional compilation flags.
# We define WIN32 on windows, and UNIX on Linux/OS X/Unix
#
# On Windows box we must link to socket library!
# -------------------------------------------------------------
if(WIN32)
	set(LIBRARIES ${LIBRARIES} wsock32 ws2_32)
    add_definitions(-DWIN32)
elseif(UNIX)
    add_definitions(-DUNIX)
endif()

# -------------------------------------------------------------

# Add project include directory to INCLUDES
# Group include files into INC_FILES
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
file(GLOB INC_FILES ${PROJECT_INCLUDE_DIR}/*.hpp)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})

# Get project files used to create executable
file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)

# Get project data files (currently only 1 file)
set(DATA_FILES ${PROJECT_SOURCE_DIR}/data/config.txt)

# SPECIFY TARGET
add_library(CCMBridge STATIC ${SRC_FILES} ${INC_FILES} ${DATA_FILES})

# Set project includes and lib directories
include_directories(PUBLIC ${INCLUDES})
target_link_libraries(CCMBridge ${LIBRARIES})

# Create source groups - this is only needed to create
# neat folder structure in Visual Studio...
source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${INC_FILES})

# Copy data file to output directory
configure_file(${DATA_FILES} ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
