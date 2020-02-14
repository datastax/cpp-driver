file(GLOB SOURCES *.cpp *.hpp)

source_group("Source Files\\" REGULAR_EXPRESSION ".*\\.c(pp)?")
source_group("Header Files\\" REGULAR_EXPRESSION ".*\\.h(pp)?")

if(CASS_BUILD_SHARED)
  add_library(curl_hostcheck OBJECT ${SOURCES})
  set_target_properties(curl_hostcheck PROPERTIES 
    POSITION_INDEPENDENT_CODE ON 
    FOLDER "Driver/Dependencies")
endif()

if(CASS_BUILD_STATIC)
  add_library(curl_hostcheck_static OBJECT ${SOURCES})
  set_target_properties(curl_hostcheck_static PROPERTIES 
    FOLDER "Driver/Dependencies")
endif()

