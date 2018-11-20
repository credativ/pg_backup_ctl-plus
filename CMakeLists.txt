cmake_minimum_required (VERSION 3.0)
project (pg_backup_ctl++)

##
## New IN_LIST() operator requires this.
##
if(CMAKE_VERSION VERSION_GREATER "3.0")
cmake_policy(SET CMP0054 NEW)
endif()

if (CMAKE_VERSION VERSION_GREATER "3.2")
cmake_policy(SET CMP0057 NEW)
endif()

##
## Currently we want debug enabled builds
##
set(CMAKE_BUILD_TYPE Debug)

##
## Compiled in sqlite catalog database filename.
##
set(PG_BACKUP_CTL_SQLITE "~/.pg_backup_ctl.sqlite" CACHE FILEPATH "path to sqlite database")
message("sqlite database will be located in ${PG_BACKUP_CTL_SQLITE}")

##
## Not sure this is a good idea, but i want
## to force C++11
##
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## looks like older cmake needs this, too:
add_definitions(-std=c++11)
add_definitions(-Wall)

##
## Additional modules for CMake
##
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/Modules" "${CMAKE_MODULE_PATH}")

##
## Special rpath setting for OSX
##
set(CMAKE_MACOSX_RPATH 1)

##
## Don't use RPATH set to build path
##
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

##
## Version string
##
set (PG_BACKUP_CTL_MAJOR 0)
set (PG_BACKUP_CTL_MINOR 1)

##
## Configure targets
##
include_directories ("${PROJECT_SOURCE_DIR}/include/")
include_directories ("${PROJECT_SOURCE_DIR}/include/jobs")
include_directories ("${PROJECT_SOURCE_DIR}/include/filesystem")
include_directories ("${PROJECT_SOURCE_DIR}/include/catalog")
include_directories ("${PROJECT_SOURCE_DIR}/include/parser")
include_directories ("${PROJECT_SOURCE_DIR}/include/backup")
include_directories ("${PROJECT_SOURCE_DIR}/include/recovery")
add_executable (pg_backup_ctl++ src/main/tab_completion.cxx src/main/pg_backup_ctl.cxx)
add_library (pgbckctl-common SHARED src/main/rtconfig.cxx src/main/common.cxx
  src/jobs/signalhandler.cxx src/jobs/daemon.cxx
  src/filesystem/fs-archive.cxx src/catalog/catalog.cxx src/catalog/retention.cxx
  src/parser/parser.cxx src/parser/commands.cxx src/backup/xlogdefs.cxx
  src/backup/backup.cxx src/backup/stream.cxx src/backup/backupprocesses.cxx
  src/recovery/restore.cxx)

IF(APPLE)
   target_link_libraries (pg_backup_ctl++ pgbckctl-common)
ELSE()
   target_link_libraries (pg_backup_ctl++ rt pgbckctl-common)
ENDIF()

# Detect if the "port" command is valid on this system; if so, return full path
EXECUTE_PROCESS(COMMAND which port RESULT_VARIABLE DETECT_MACPORTS OUTPUT_VARIABLE MACPORTS_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${DETECT_MACPORTS} EQUAL 0)
    # "/opt/local/bin/port" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local/bin" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local" is where MacPorts lives, add `/lib` suffix and link
    link_directories("${MACPORTS_PREFIX}/lib")
    include_directories("${MACPORTS_PREFIX}/include")
    target_link_libraries (pgbckctl-common z)
    MESSAGE("MacPorts libraries from ${MACPORTS_PREFIX}/lib")

    # iff we have macports, we need to lookup for libreadline there, which
    # isn't shipped with Mac (it's libedit there which we can't
    # use). Since the check above already added the macports
    # library path for library search during linking, we just add
    # libreadline explicitely to link to when we find it.
    find_library(MACPORTS_LIBREADLINE readline NO_DEFAULT_PATH PATHS ${MACPORTS_PREFIX}/include ${MACPORTS_PREFIX}/lib)
    message("macports provides libreadline ${MACPORTS_LIBREADLINE}")
    target_link_libraries(pg_backup_ctl++ ${MACPORTS_LIBREADLINE})
ELSE()

    ##
    ## We need readline
    ##
    find_package (readline REQUIRED)

    if (readline_FOUND)
       include_directories(${readline_INCLUDE_DIRS})
       message("linking libreadline ${readline_LIBRARIES}")
       target_link_libraries (pg_backup_ctl++ ${readline_LIBRARIES})
    endif (readline_FOUND)
ENDIF()

##
## Check shmctl() and shmatt_t datatype if available
##
## NOTE: FreeBSD doesn't have this defined, so make
##       a specific workaround in pg_backup_ctl.hxx in this case.
##
INCLUDE(CheckTypeSize)
set(CMAKE_EXTRA_INCLUDE_FILES "sys/shm.h")
CHECK_TYPE_SIZE(shmatt_t SHMATT_T_SZ LANGUAGE C)
if (SHMATT_T_SZ STREQUAL "")
  message("define shmatt_t to int")
  set(HAVE_SHMATT_T_TYPE "#undef HAVE_SHMATT_T_TYPE")
else()
  message("using shmatt_t")
  set(HAVE_SHMATT_T_TYPE "#define HAVE_SHMATT_T_TYPE")
endif()

##
## Check endianess of target system
##
include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
  message(STATUS "detected big endian machine")
  set(PG_BACKUP_CTL_BIG_ENDIAN "#define PG_BACKUP_CTL_BIG_ENDIAN 1")
else()
  message(STATUS "detected little endian machine")
  set(PG_BACKUP_CTL_BIG_ENDIAN "#undef PG_BACKUP_CTL_BIG_ENDIAN")
endif()

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
find_package(Boost REQUIRED COMPONENTS filesystem regex system date_time iostreams)
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
else()
   message(FATAL_ERROR "libpq development header and PostgreSQL server includes required")
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
   message("linking zlib in ${zlib_LIBRARIES}")
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

## This is for CPack, which
## helps to create tarball distribution files.
set(CPACK_PACKAGE_VERSION_MAJOR 0)
set(CPACK_PACKAGE_VERSION_MINOR 1)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "An advanced PostgreSQL streaming backup tool")
set(CPACK_PACKAGE_VENDOR "credativ GmbH")
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_CURRENT_SOURCE_DIR}/README)
set(CPACK_GENERATOR_TGZ)
set(
  CPACK_SOURCE_PACKAGE_FILE_NAME
  "pg_backup_ctl-plus-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}"
  CACHE INTERNAL "tarball basename"
  )
set(CPACK_SOURCE_IGNORE_FILES
  "~$"
  "${PROJECT_SOURCE_DIR}/\\\\.git"
  "\\\\.gitignore"
  "\\\\.gitlab-ci.yml"
  "Makefile"
  "build/\\\\.*"
  )
include (CPack)

##
## Installation targets
##
include(GNUInstallDirs)
install(PROGRAMS build/pg_backup_ctl++ DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES build/libpgbckctl-common.so DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES src/sql/catalog.sql DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/pg_backup_ctl-plus)

##
## If build system wants to have the systemd service
## files installed, tell CPack accordingly.
##
if(DEFINED SYSTEMD_SERVICE_FILE)
message("installing system service files in ${SYSTEMD_SERVICE_FILE}")
install(FILES resources/systemd/pgbckctl-launcher.service DESTINATION ${SYSTEMD_SERVICE_FILE})
install(FILES resources/systemd/pgbckctl-walstreamer@.service DESTINATION ${SYSTEMD_SERVICE_FILE})
install(FILES resources/systemd/pg_backup_ctl-plus-tempfiles.conf DESTINATION /usr/lib/tmpfiles.d/)
endif()

##
## Get current git revision.
##
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
message("looks like a git source tree, setting git commit hash to includes")
execute_process(COMMAND git describe --dirty --tags --always
                OUTPUT_VARIABLE GITREV)
else()
message("setting GITREV to package version ${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}")
set(GITREV "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}")
endif()

configure_file (
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx.in"
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx"
)
