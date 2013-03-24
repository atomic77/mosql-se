# - Find MYSQL
# Find the native MYSQL includes and library
#
# MYSQLSRC_INCLUDES - where to find db.h
# MYSQLSRC_LIBRARIES - List of libraries when using MYSQL.
# MYSQLSRC_FOUND - True if MYSQL found.

set(MYSQLSRC_ROOT "" CACHE STRING "BerkeleyDB root directory")

find_path(MYSQLSRC_INCLUDE_DIR include/mysql_version.h HINTS "${MYSQLSRC_ROOT}/")
#find_path(MYSQLSRC_INCLUDE_DIR mysql_version.h HINTS "${MYSQLSRC_ROOT}/include ${MYSQLSRC_ROOT}/include/mysql")
#find_library(MYSQLSRC_LIBRARY mysqld mysqld.a HINTS "${MYSQLSRC_ROOT}/lib")

message(STATUS "Using ${MYSQLSRC_INCLUDE_DIR} for base mysql includes")
#set(MYSQLSRC_LIBRARIES ${MYSQLSRC_LIBRARY})
set(MYSQLSRC_INCLUDE_DIRS ${MYSQLSRC_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
#find_package_handle_standard_args(MYSQL DEFAULT_MSG MYSQLSRC_LIBRARY MYSQLSRC_INCLUDE_DIR)
find_package_handle_standard_args(MYSQL DEFAULT_MSG MYSQLSRC_INCLUDE_DIR)

#mark_as_advanced(MYSQLSRC_INCLUDE_DIR MYSQLSRC_LIBRARY)
mark_as_advanced(MYSQLSRC_INCLUDE_DIR )
