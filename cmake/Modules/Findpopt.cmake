#
# Submodule for CMake to find libpopt
#
# Sets the following variables:
# - popt_FOUND: libpopt was found
# - popt_INCLUDE_DIRS: Include directories for libpopt
# - popt_LIBRARIES: Library directories for libpopt
#

find_path(popt_INCLUDE_DIR
  NAMES "popt.h"
  DOC "popt include header files")
mark_as_advanced(popt_INCLUDE_DIR)

# find libraries
find_library(popt_LIBRARY "popt"
  DOC "popt command line argument parsing library")
mark_as_advanced(popt_LIBRARY)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(popt
  FOUND_VAR popt_FOUND
  REQUIRED_VARS popt_INCLUDE_DIR
  FAIL_MESSAGE "Failed to get popt library")

if(popt_FOUND)
  set(popt_INCLUDE_DIRS "${popt_INCLUDE_DIR}")
  if (popt_LIBRARY)
    set(popt_LIBRARIES "${popt_LIBRARY}")
  else()
    unset(popt_LIBRARIES)
  endif()
endif()
