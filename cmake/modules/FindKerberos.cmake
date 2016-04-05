include(FindPackageHandleStandardArgs)

find_path(LIBKRB5_INCLUDE_DIR
  NAMES gssapi.h
  HINTS ${_LIBKRB5_INCLUDEDIR} ${_LIBKRB5_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)

find_library(LIBKRB5_LIBRARY
  NAMES krb5 libkrb5
  HINTS ${_LIBKRB5_LIBDIR} ${_LIBKRB5_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)

find_package_handle_standard_args(KerberosV5 "Could NOT find krb5, try to set the path to the krb5 root folder in the system variable LIBKRB5_ROOT_DIR"
  LIBKRB5_LIBRARY
  LIBKRB5_INCLUDE_DIR)
mark_as_advanced(LIBKRB5_LIBRARY LIBKRB5_INCLUDE_DIR)
