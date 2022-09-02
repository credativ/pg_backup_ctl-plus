#
# Submodule for CMake to find libpq
#
# Sets the following variables:
# - liburing_FOUND: libpq was found
# - liburing_INCLUDE_DIRS: Include directories for libpq
# - liburing_LIBRARIES: Library directories for libpq
#

# find includes
find_path(liburing_INCLUDE_DIR
  NAMES "liburing.h"
  DOC "liburing header files")
mark_as_advanced(liburing_INCLUDE_DIR)

# find shared libraries
find_library(liburing_LIBRARY "uring"
  DOC "liburing shared library")
mark_as_advanced(liburing_LIBRARIES)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(liburing
  FOUND_VAR liburing_FOUND
  REQUIRED_VARS liburing_INCLUDE_DIR
  FAIL_MESSAGE "Failed to get liburing library for io_uring support")

if (liburing_FOUND)
  set(liburing_INCLUDE_DIRS "${liburing_INCLUDE_DIR}")
  if (liburing_LIBRARY)
    set(liburing_LIBRARIES "${liburing_LIBRARY}")
  else()
    unset(liburing_LIBRARIES)
  endif()
endif()
