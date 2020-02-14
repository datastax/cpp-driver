file(GLOB SOURCES *.cpp *.hpp)

source_group("Source Files\\" REGULAR_EXPRESSION ".*\\.c(pp)?")
source_group("Header Files\\" REGULAR_EXPRESSION ".*\\.h(pp)?")

if(CASS_BUILD_SHARED)
  add_library(hdr_histogram OBJECT ${SOURCES})
  set_target_properties(hdr_histogram PROPERTIES 
    POSITION_INDEPENDENT_CODE ON
    FOLDER "Driver/Dependencies")
endif()

if(CASS_BUILD_STATIC)
  add_library(hdr_histogram_static OBJECT ${SOURCES})
  set_target_properties(hdr_histogram_static PROPERTIES 
    FOLDER "Driver/Dependencies")
endif()
