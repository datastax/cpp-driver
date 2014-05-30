cmake_minimum_required(VERSION 2.6.4)

project(CCMBridge C CXX)

# Allow access to custom FindLIBSSH2 module
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

set(INCLUDES "")
set(LIBRARIES "")

# Find boost log library 
# -------------------------------------------------------------
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_STATIC_RUNTIME OFF)
set(Boost_USE_MULTITHREADED ON)

# needed by boost to link dynamically
add_definitions(-DBOOST_ALL_DYN_LINK)

find_package(Boost 1.41.0 COMPONENTS system date_time REQUIRED)
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})

# boost log depends on boost thread
find_package(Boost COMPONENTS log_setup log thread)
if(NOT Boost_FOUND)
    add_definitions(-DCQL_NO_BOOST_LOG)
else()
    set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})
    set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})
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
find_package(ZLIB REQUIRED)

set(LIBRARIES ${LIBRARIES} ${ZLIB_LIBRARIES})
set(INCLUDES ${INCLUDES} ${ZLIB_INCLUDE_DIR})
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
