file(GLOB SOURCES http_parser.c http_parser.h)

source_group("Source Files\\" REGULAR_EXPRESSION ".*\\.c(pp)?")
source_group("Header Files\\" REGULAR_EXPRESSION ".*\\.h(pp)?")

if(CASS_BUILD_SHARED)
  add_library(http-parser OBJECT ${SOURCES})
  set_target_properties(http-parser PROPERTIES 
    POSITION_INDEPENDENT_CODE ON
    FOLDER "Driver/Dependencies")
endif()

if(CASS_BUILD_STATIC)
  add_library(http-parser_static OBJECT ${SOURCES})
  set_target_properties(http-parser_static PROPERTIES 
    FOLDER "Driver/Dependencies")
endif()
