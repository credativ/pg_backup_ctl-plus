#
# Submodule for CMake to find libzstd
#
# Sets the following variables:
# - zstd_FOUND: libsqlite3 was found
# - zstd_INCLUDE_DIRS: Include directories for libsqlite3
# - zstd_LIBRARIES: Library directories for libsqlite3
#

find_path(zstd_INCLUDE_DIR
  NAMES "zstd.h"
  DOC "zstd include header files")
mark_as_advanced(zstd_INCLUDE_DIR)

# find libraries
find_library(zstd_LIBRARY "zstd"
  DOC "zstandard compression library")
mark_as_advanced(zstd_LIBRARY)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(zstd
  FOUND_VAR zstd_FOUND
  REQUIRED_VARS zstd_INCLUDE_DIR
  FAIL_MESSAGE "Failed to get zstd library")

if(zstd_FOUND)
  set(zstd_INCLUDE_DIRS "${zstd_INCLUDE_DIR}")
  if (zstd_LIBRARY)
    set(zstd_LIBRARIES "${zstd_LIBRARY}")
  else()
    unset(zstd_LIBRARIES)
  endif()
endif()
