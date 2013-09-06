cmake_minimum_required(VERSION 2.8)

project(CCMBridge C CXX)

# Allow access to custom FindLIBSSH2 module
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(LIBSSH2 REQUIRED)

message(STATUS ${LIBSSH2_FOUND})
message(STATUS ${LIBSSH2_INCLUDE_DIRS})
message(STATUS ${LIBSSH2_LIBRARIES})
message(STATUS "lib path:: " ${CMAKE_LIBRARY_PATH})

include_directories(${LIBSSH2_INCLUDE_DIRS})
link_libraries(${LIBSSH2_LIBRARIES})

add_executable(testSsh src/ssh2.c)
