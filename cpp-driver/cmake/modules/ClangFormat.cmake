#
# Format and verify formatting using clang-format
#
cmake_minimum_required(VERSION 2.6.4)

include(FindPackageHandleStandardArgs)

if(NOT CLANG_FORMAT_EXE_NAME)
  set(CLANG_FORMAT_EXE_NAME clang-format)
endif()

if(CLANG_FORMAT_ROOT_DIR)
  find_program(CLANG_FORMAT_EXE
    NAMES ${CLANG_FORMAT_EXE_NAME}
    PATHS ${CLANG_FORMAT_ROOT_DIR}
    NO_DEFAULT_PATH)
endif()

find_program(CLANG_FORMAT_EXE NAMES ${CLANG_FORMAT_EXE_NAME})

find_package_handle_standard_args(CLANG_FORMAT DEFAULT_MSG CLANG_FORMAT_EXE)

mark_as_advanced(CLANG_FORMAT_EXE)

if(CLANG_FORMAT_FOUND)
  set(CLANG_FORMAT_FILE_EXTENSIONS ${CLANG_FORMAT_CXX_FILE_EXTENSIONS} *.cpp *.hpp *.c *.h)
  file(GLOB_RECURSE CLANG_FORMAT_ALL_SOURCE_FILES ${CLANG_FORMAT_FILE_EXTENSIONS})

  set(CLANG_FORMAT_EXCLUDE_PATTERNS ${CLANG_FORMAT_EXCLUDE_PATTERNS} "/CMakeFiles/" "cmake" "/build/" "/vendor/" "/third_party/" "cassandra.h" "dse.h")

  foreach (SOURCE_FILE ${CLANG_FORMAT_ALL_SOURCE_FILES})
    foreach (EXCLUDE_PATTERN ${CLANG_FORMAT_EXCLUDE_PATTERNS})
      string(FIND ${SOURCE_FILE} ${EXCLUDE_PATTERN} EXCLUDE_FOUND)
      if (NOT ${EXCLUDE_FOUND} EQUAL -1)
        list(REMOVE_ITEM CLANG_FORMAT_ALL_SOURCE_FILES ${SOURCE_FILE})
      endif ()
    endforeach ()
  endforeach ()

  add_custom_target(format
    COMMENT "Format source files using clang-format"
    COMMAND ${CLANG_FORMAT_EXE} -i -fallback-style=none -style=file ${CLANG_FORMAT_ALL_SOURCE_FILES})

  add_custom_target(format-check
    COMMENT "Verify source files formatting using clang-format"
    COMMAND ! ${CLANG_FORMAT_EXE} -output-replacements-xml -fallback-style=none -style=file ${CLANG_FORMAT_ALL_SOURCE_FILES} | tee replacements.xml | grep -q "replacement offset")
else()
  message(STATUS "Unable to find clang-format. Not creating format targets.")
endif()
