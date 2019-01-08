cmake_minimum_required(VERSION 3.10)

set(_LIBCLANG_ROOT_HINTS ${LIBCLANG_ROOT_DIR}
  ENV LIBCLANG_ROOT_DIR
  ${_LIBCLANG_ROOT_HINTS})

set(_LIBCLANG_ROOT_HINTS_AND_PATHS
  HINTS ${_LIBCLANG_ROOT_HINTS}
  PATHS ${_LIBCLANG_ROOT_PATHS})

set(_LIBCLANG_INCLUDE_NAMES
  clang/Config/config.h
  llvm/Config/llvm-config.h)

find_path(LIBCLANG_INCLUDE_DIR
  NAMES ${_LIBCLANG_INCLUDE_NAMES}
  ${_LIBCLANG_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)

# Order matters here
set(_LIBCLANG_LIBRARY_NAMES
  LLVM
  clang
  clangASTMatchers
  clangTooling
  clangDriver
  clangFrontend
  clangSerialization
  clangParse
  clangSema
  clangEdit
  clangAST
  clangAnalysis
  clangLex
  clangBasic)

foreach(NAME ${_LIBCLANG_LIBRARY_NAMES})
  find_library(LIBCLANG_${NAME}
    NAMES ${NAME}
    ${_LIBCLANG_ROOT_HINTS_AND_PATHS}
    PATH_SUFFIXES lib lib/${CMAKE_LIBRARY_ARCHITECTURE}
    NO_DEFAULT_PATH)
  set(LIBCLANG_LIBRARIES ${LIBCLANG_LIBRARIES} ${LIBCLANG_${NAME}})
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libclang
  REQUIRED_VARS
  LIBCLANG_LLVM
  LIBCLANG_clang
  LIBCLANG_clangASTMatchers
  LIBCLANG_clangTooling
  LIBCLANG_clangDriver
  LIBCLANG_clangFrontend
  LIBCLANG_clangSerialization
  LIBCLANG_clangParse
  LIBCLANG_clangSema
  LIBCLANG_clangEdit
  LIBCLANG_clangAST
  LIBCLANG_clangAnalysis
  LIBCLANG_clangLex
  LIBCLANG_clangBasic
  LIBCLANG_INCLUDE_DIR)

add_executable(alloc-linter-tool alloc_linter_tool.cpp)

target_include_directories(alloc-linter-tool
  PRIVATE ${LIBCLANG_INCLUDE_DIR})

target_link_libraries(alloc-linter-tool
  PRIVATE ${LIBCLANG_LIBRARIES})
