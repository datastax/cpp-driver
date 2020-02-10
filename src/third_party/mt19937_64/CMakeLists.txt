file(GLOB SOURCES *.hpp)

source_group("Source Files\\" REGULAR_EXPRESSION ".*\\.c(pp)?")
source_group("Header Files\\" REGULAR_EXPRESSION ".*\\.h(pp)?")

add_custom_target(mt19937_64 ${SOURCES})
set_target_properties(mt19937_64 PROPERTIES 
  FOLDER "Driver/Dependencies")
