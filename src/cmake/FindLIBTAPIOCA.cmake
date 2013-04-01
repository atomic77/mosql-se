# - Find libpaxos
# Find the disk-based version of Ring-Paxos on the system
# LIBTAPIOCA_INCLUDES - where to find lp_learner.h
# LIBTAPIOCA_LIBRARIES - List of libraries when using BDB.
# LIBTAPIOCA_FOUND - True if libpaxos found.

set(LIBTAPIOCA_ROOT "" CACHE STRING "Where to find the mosql storage layer libraries")

find_path(LIBTAPIOCA_INCLUDE_DIR tapioca/tapioca.h HINTS "${LIBTAPIOCA_ROOT}/include")
find_library(LIBTAPIOCA_LIBRARY tapioca HINTS "${LIBTAPIOCA_ROOT}/lib")

set(LIBTAPIOCA_LIBRARIES ${LIBTAPIOCA_LIBRARY})
set(LIBTAPIOCA_INCLUDE_DIRS ${LIBTAPIOCA_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(LIBTAPIOCA DEFAULT_MSG
                                  LIBTAPIOCA_LIBRARY LIBTAPIOCA_INCLUDE_DIR)

mark_as_advanced(LIBTAPIOCA_INCLUDE_DIR LIBTAPIOCA_LIBRARY)
