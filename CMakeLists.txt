cmake_minimum_required (VERSION 2.6)
project (pg_backup_ctl++)

##
## Currently we want debug enabled builds
##
set(CMAKE_BUILD_TYPE Debug)

##
## Compiled in sqlite catalog database filename.
##
set(PG_BACKUP_CTL_SQLITE "/home/bernd/.pg_backup_ctl.sqlite" CACHE FILEPATH "path to sqlite database")
message("sqlite database will be located in ${PG_BACKUP_CTL_SQLITE}")

##
## Not sure this is a good idea, but i want
## to force C++11
##
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## looks like older cmake needs this, too:
add_definitions(-std=c++11)

##
## Additional modules for CMake
##
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/Modules" "${CMAKE_MODULE_PATH}")

##
## Special rpath setting for OSX
##
set(CMAKE_MACOSX_RPATH 1)

##
## Version string
##
set (PG_BACKUP_CTL_MAJOR 0)
set (PG_BACKUP_CTL_MINOR 1)

##
## Configure targets
##
include_directories ("${PROJECT_SOURCE_DIR}/include/")
include_directories ("${PROJECT_SOURCE_DIR}/include/filesystem")
include_directories ("${PROJECT_SOURCE_DIR}/include/catalog")
include_directories ("${PROJECT_SOURCE_DIR}/include/parser")
include_directories ("${PROJECT_SOURCE_DIR}/include/jobs")
include_directories ("${PROJECT_SOURCE_DIR}/include/backup")
add_executable (pg_backup_ctl++ src/main/tab_completion.cxx src/main/pg_backup_ctl.cxx)
add_library (pgbckctl-common SHARED src/main/common.cxx src/filesystem/fs-archive.cxx src/catalog/catalog.cxx
  src/parser/parser.cxx src/parser/commands.cxx src/jobs/daemon.cxx src/backup/backup.cxx src/backup/stream.cxx
  src/backup/backupprocesses.cxx)
target_link_libraries (pg_backup_ctl++ pgbckctl-common)

##
## We need readline
##
find_package (readline REQUIRED)
if (readline_FOUND)
  include_directories(${readline_INCLUDE_DIRS})
  message("linking libreadline ${readline_LIBRARIES}")
  target_link_libraries (pg_backup_ctl++ ${readline_LIBRARIES})
endif (readline_FOUND)

##
## We need popt
##
find_package (popt REQUIRED)
if (popt_FOUND)
  include_directories(${popt_INCLUDE_DIRS})
  message("linking libpopt ${popt_LIBRARIES}")
  target_link_libraries (pg_backup_ctl++ ${popt_LIBRARIES})
endif (popt_FOUND)

##
## We need sqlite3
##
find_package (sqlite3 REQUIRED)
if (sqlite3_FOUND)
  include_directories(${sqlite3_INCLUDE_DIRS})
  message("linking libsqlite3 ${sqlite3_LIBRARIES}")
  target_link_libraries (pgbckctl-common ${sqlite3_LIBRARIES})
endif (sqlite3_FOUND)

##
## We need Boost::filesystem
##
find_package(Boost REQUIRED COMPONENTS regex filesystem system date_time iostreams)
if (Boost_FOUND)
   include_directories(${Boost_INCLUDE_DIRS})
   message("linking Boost ${Boost_LIBRARIES}")
   target_link_libraries (pgbckctl-common ${Boost_LIBRARIES})
endif(Boost_FOUND)

##
## We need PostgreSQL libpq
##
find_package(libpq REQUIRED)
if (libpq_FOUND)
   include_directories(${libpq_INCLUDE_DIRS})
   include_directories(${pgsql_INCLUDE_SERVER})
   message("linking PostgreSQL libpq ${libpq_LIBRARIES}")
   target_link_libraries (pgbckctl-common ${libpq_LIBRARIES})
endif(libpq_FOUND)

##
## Force GNU/gettext
##
find_package(Gettext)
if (GETTEXT_FOUND)
   message("GNU/gettext support available")
else()
   message(SEND_ERROR "gettext support required, but no gettext found")
endif(GETTEXT_FOUND)

##
## We want to have zlib, ideally
##
find_package(ZLIB OPTIONAL_COMPONENTS)
if(ZLIB_FOUND)
   message("using zlib support")
   set(PG_BACKUP_CTL_HAS_ZLIB "#define PG_BACKUP_CTL_HAS_ZLIB 1")
   include_directories(${zlib_INCLUDE_DIRS})
   target_link_libraries(pgbckctl-common ${zlib_LIBRARIES})
else()
   message("no zlib found, compression unavailable")
   set(PG_BACKUP_CTL_HAS_ZLIB "#undef PG_BACKUP_CTL_HAS_ZLIB")
endif()

##
## Checkout, if zstandard compression library is available
##
find_package(zstd OPTIONAL_COMPONENTS)
if(ZSTD_FOUND)
  message("using zstandard compression support")
  set(PG_BACKUP_CTL_HAS_ZSTD "#define PG_BACKUP_CTL_HAS_ZSTD 1")
  include_directories(${zstd_INCLUDE_DIRS})
  target_link_libraries(pgbckctl-common ${zstd_LIBRARIES})
else()
  message("no zstd library found, zstandard compression unavailable")
  set(PG_BACKUP_CTL_HAS_ZSTD "#undef PG_BACKUP_CTL_HAS_ZSTD")
endif()

##
## Configure doxygen and a custom target "doc"
## to build documentation
##
find_package(Doxygen OPTIONAL_COMPONENTS)
if(DOXYGEN_FOUND)
  message("doxygen documentation processor found")
  set(DOXYGEN_OUTPUT_DIRECTORY ../doc)

  if(${CMAKE_VERSION} VERSION_LESS "3.9.0")
    message("automatic doxygen support not available, please consider to switch to CMake 3.9.0")
  else()
    doxygen_add_docs(doc src include)
  endif()

endif()

##
## Get current git revision.
##
execute_process(COMMAND git describe --dirty --tags --always
                OUTPUT_VARIABLE GITREV)

configure_file (
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx.in"
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx"
)
