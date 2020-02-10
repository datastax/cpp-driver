set(INCLUDES ${CASS_INCLUDE_DIR})

file(GLOB_RECURSE EXAMPLES_TO_BUILD "*/CMakeLists.txt")

#------------------------------
# Compiler flags
#------------------------------

if(CASS_USE_STATIC_LIBS)
  add_definitions(-DCASS_STATIC)
endif()

#------------------------
# Subdirectories
#------------------------

list(APPEND INCLUDES ${CASS_INCLUDES})

foreach(example ${EXAMPLES_TO_BUILD})
  get_filename_component(exdir ${example} PATH)
  add_subdirectory(${exdir})
endforeach()
