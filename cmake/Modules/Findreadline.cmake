#
# Submodule for CMake to find libreadline
#
# Sets the following variables:
# - readline_FOUND: libreadline was found
# - readline_INCLUDE_DIRS: Include directories for libreadline
# - readline_LIBRARIES: Library directories for libreadline
#

find_path(readline_INCLUDE_DIR
  NAMES "readline/readline.h"
  DOC "readline include header files")
mark_as_advanced(readline_INCLUDE_DIR)

# find libraries
find_library(readline_LIBRARY "readline"
  DOC "readline command line argument parsing library")
mark_as_advanced(readline_LIBRARY)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(readline
  FOUND_VAR readline_FOUND
  REQUIRED_VARS readline_INCLUDE_DIR
  FAIL_MESSAGE "Failed to get readline library")

if(readline_FOUND)
  set(readline_INCLUDE_DIRS "${readline_INCLUDE_DIR}")
  if (readline_LIBRARY)
    set(readline_LIBRARIES "${readline_LIBRARY}")
  else()
    unset(readline_LIBRARIES)
  endif()
endif()
