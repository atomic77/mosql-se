# - Find MYSQL
# Find the native MYSQL includes and library
#
# MYSQL_INCLUDES - where to find db.h
# MYSQL_LIBRARIES - List of libraries when using MYSQL.
# MYSQL_FOUND - True if MYSQL found.

set(MYSQL_ROOT "" CACHE STRING "BerkeleyDB root directory")

find_library(MYSQL_LIBRARY mysqlclient HINTS "${MYSQL_ROOT}/lib" "${MYSQL_ROOT}/lib/mysql")
find_library(MYSQL_LIBRARY_R mysqlclient_r HINTS "${MYSQL_ROOT}/lib" "${MYSQL_ROOT}/lib/mysql")

#message(STATUS "Using ${MYSQL_INCLUDE_DIR} for base mysql includes")
set(MYSQL_LIBRARIES ${MYSQL_LIBRARY} ${MYSQL_LIBRARY_R}   )
set(MYSQL_INCLUDE_DIRS ${MYSQL_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(MYSQL DEFAULT_MSG MYSQL_LIBRARY )

mark_as_advanced(MYSQL_LIBRARY)
