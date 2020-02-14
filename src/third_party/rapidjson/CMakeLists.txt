file(GLOB RAPIDJSON_HEADER_FILES rapidjson/*.h)
file(GLOB RAPIDJSON_ERROR_HEADER_FILES rapidjson/error/*.h)
file(GLOB RAPIDJSON_INTERNAL_HEADER_FILES rapidjson/internal/*.h)
file(GLOB RAPIDJSON_MSIINTTYPES_HEADER_FILES rapidjson/msiinttypes/*.h)

set(SOURCES 
  ${RAPIDJSON_HEADER_FILES} 
  ${RAPIDJSON_ERROR_HEADER_FILES} 
  ${RAPIDJSON_INTERNAL_HEADER_FILES} 
  ${RAPIDJSON_MSIINTTYPES_HEADER_FILES})

source_group("Header Files\\" FILES ${RAPIDJSON_HEADER_FILES})
source_group("Header Files\\error" FILES ${RAPIDJSON_ERROR_HEADER_FILES})
source_group("Header Files\\internal" FILES ${RAPIDJSON_INTERNAL_HEADER_FILES})
source_group("Header Files\\msiinttypes" FILES ${RAPIDJSON_MSIINTTYPES_HEADER_FILES})

add_custom_target(rapidjson ${SOURCES})
set_target_properties(rapidjson PROPERTIES 
  FOLDER "Driver/Dependencies")
