# - Find PCRE
# Find the native LibEvent includes and library
#
# PCRE_INCLUDES - where to find event.h
# PCRE_LIBRARIES - List of libraries when using PCRE.
# PCRE_FOUND - True if LibEvent found.

set(PCRE_ROOT "" CACHE STRING "PCRE root directory")

find_path(PCRE_INCLUDE_DIR pcre.h 
    HINTS "${PCRE_ROOT}/include")

find_library(PCRE_LIBRARY
   NAMES pcre
   HINTS "${PCRE_ROOT}/lib")

set(PCRE_LIBRARIES ${PCRE_LIBRARY})
set(PCRE_INCLUDE_DIRS ${PCRE_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set PCRE_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(PCRE DEFAULT_MSG
                                  PCRE_LIBRARY PCRE_INCLUDE_DIR)

mark_as_advanced(PCRE_INCLUDE_DIR PCRE_LIBRARY)
