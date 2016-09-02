cmake_minimum_required (VERSION 2.6)
project (pg_backup_ctl++)

##
## Currently we want debug enabled builds
##
set(CMAKE_BUILD_TYPE Debug)

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
add_executable (pg_backup_ctl++ src/main/pg_backup_ctl.cxx)
add_library (pgbckctl-common SHARED src/main/common.cxx src/filesystem/fs-archive.cxx)
target_link_libraries (pg_backup_ctl++ pgbckctl-common)

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
find_package(Boost REQUIRED COMPONENTS filesystem system)
if (Boost_FOUND)
   include_directories(${Boost_INCLUDE_DIRS})
   message("linking Boost ${Boost_LIBRARIES}")
   target_link_libraries (pgbckctl-common ${Boost_LIBRARIES})
endif(Boost_FOUND)

##
## Get current git revision.
##
execute_process(COMMAND git describe --dirty --tags --always
                OUTPUT_VARIABLE GITREV)

configure_file (
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx.in"
   "${PROJECT_SOURCE_DIR}/include/pg_backup_ctl.hxx"
)
