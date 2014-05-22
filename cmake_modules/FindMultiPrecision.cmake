set(MULTIPRECISION_INLCUDE_DIR ${Boost_INCLUDE_DIRS}/boost/multiprecision)
find_path(MULTIPRECISION_FOUND_FILE cpp_int.hpp NO_DEFAULT_PATH PATHS ${MULTIPRECISION_INLCUDE_DIR})

if(MULTIPRECISION_FOUND_FILE)
    set(multiprecision_FOUND TRUE)
else()
    set(multiprecision_FOUND FALSE)
endif()

mark_as_advanced(multiprecision_FOUND)
