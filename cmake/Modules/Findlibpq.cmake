#
# Submodule for CMake to find libpq
#
# Sets the following variables:
# - libpq_FOUND: libpq was found
# - libpq_INCLUDE_DIRS: Include directories for libpq
# - pgsql_INCLUDE_SERVER: Include directory for server API
# - libpq_LIBRARIES: Library directories for libpq
#

# Rely on pg_config to get all necessary includes and libs
find_program(PG_CONFIG NAMES pg_config
                       DOCS "Path to pg_config utility")

if (PG_CONFIG)
   exec_program(${PG_CONFIG} ARGS "--version"
      OUTPUT_VARIABLE PG_CONFIG_VERSION)
   message("pg_config version ${PG_CONFIG_VERSION}")

   ## Get INCLUDEDIR
   exec_program(${PG_CONFIG} ARGS "--includedir"
      OUTPUT_VARIABLE PG_CONFIG_INCLUDEDIR)
   message("PostgreSQL includedir ${PG_CONFIG_INCLUDEDIR}")

   ## Get PostgreSQL server includes
   exec_program(${PG_CONFIG} ARGS "--includedir-server"
      OUTPUT_VARIABLE PG_CONFIG_INCLUDEDIR_SERVER)
   message("PostgreSQL server includedir ${PG_CONFIG_INCLUDEDIR_SERVER}")

   ## Get LIBDIR
   exec_program(${PG_CONFIG} ARGS "--libdir"
      OUTPUT_VARIABLE PG_CONFIG_LIBDIR)
   message("PostgreSQL libdir ${PG_CONFIG_LIBDIR}")
else()
   message("pg_config not found")
endif()

find_path(libpq_INCLUDE_DIRS libpq-fe.h
   ${PG_CONFIG_INCLUDEDIR})

find_path(pgsql_INCLUDE_SERVER postgres_fe.h
   ${PG_CONFIG_INCLUDEDIR_SERVER})

find_library(libpq_LIBRARIES NAMES pq libpq
  PATHS ${PG_CONFIG_LIBDIR})

if (libpq_INCLUDE_DIRS AND libpq_LIBRARIES AND pgsql_INCLUDE_SERVER)
   set(libpq_FOUND TRUE)
else()
   set(libpq_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PostgreSQL
   DEFAULT_MSG
   libpq_INCLUDE_DIRS
   pgsql_INCLUDE_SERVER
   libpq_LIBRARIES)
