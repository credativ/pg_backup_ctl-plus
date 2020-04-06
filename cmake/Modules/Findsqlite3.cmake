#
# Submodule for CMake to find libsqlite3
#
# Sets the following variables:
# - sqlite3_FOUND: libsqlite3 was found
# - sqlite3_INCLUDE_DIRS: Include directories for libsqlite3
# - sqlite3_LIBRARIES: Library directories for libsqlite3
# - SQLITE3: Full path to sqlite3 command line utility
#
find_program(SQLITE3 NAMES sqlite3
                     DOCS "Path to sqlite3 command line utility")

if (SQLITE3)
  message("sqlite3 cmdline utility in ${SQLITE3}")
endif()

find_path(sqlite3_INCLUDE_DIR
  NAMES "sqlite3.h"
  DOC "sqlite3 include header files")
mark_as_advanced(sqlite3_INCLUDE_DIR)

# find libraries
find_library(sqlite3_LIBRARY "sqlite3"
  DOC "sqlite3 command line argument parsing library")
mark_as_advanced(sqlite3_LIBRARY)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(sqlite3
  FOUND_VAR sqlite3_FOUND
  REQUIRED_VARS sqlite3_INCLUDE_DIR
  FAIL_MESSAGE "Failed to get sqlite3 library")

if(sqlite3_FOUND)
  set(sqlite3_INCLUDE_DIRS "${sqlite3_INCLUDE_DIR}")
  if (sqlite3_LIBRARY)
    set(sqlite3_LIBRARIES "${sqlite3_LIBRARY}")
  else()
    unset(sqlite3_LIBRARIES)
  endif()
endif()
