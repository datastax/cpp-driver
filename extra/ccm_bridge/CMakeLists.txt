cmake_minimum_required(VERSION 2.8)

project(CCMBridge C CXX)

# Allow access to custom FindLIBSSH2 module
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

set(INCLUDES "")
set(LIBRARIES "")

# Find boost log library 
# -------------------------------------------------------------
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_STATIC_RUNTIME OFF)
set(Boost_USE_MULTITHREADED ON)

# needed by boost to link dynamically
add_definitions(-DBOOST_ALL_DYN_LINK)

# boost log depends on boost thread
find_package(Boost 1.54.0 COMPONENTS thread log REQUIRED)

set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})
set(INCLUDES ${INCLUDES} ${Boost_INCLUDE_DIRS})

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

# On Windows $#%#%$# box we must link to socket library!
# -------------------------------------------------------------
if(WIN32)
	set(LIBRARIES ${LIBRARIES} wsock32 ws2_32)
endif()

# -------------------------------------------------------------

# Add project include directory to INCLUDES
# Group include files into INC_FILES
set(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
file(GLOB INC_FILES ${PROJECT_INCLUDE_DIR}/*.hpp)
set(INCLUDES ${INCLUDES} ${PROJECT_INCLUDE_DIR})

# Get project files used to create executable
file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp )

# SPECIFY TARGET
# Change to STATIC library
add_executable(CCMBridge ${SRC_FILES} ${INC_FILES})

# Set project includes and lib directories
target_include_directories(CCMBridge PUBLIC ${INCLUDES})
target_link_libraries(CCMBridge ${LIBRARIES})

# Create source groups - this is only needed to create
# neat folder structure in Visual Studio...
source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${INC_FILES})
