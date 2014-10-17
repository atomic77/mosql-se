include_directories(${LIBTAPIOCA_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})
include_directories(${LIBUUID_INCLUDE_DIRS})
include_directories(${PCRE_INCLUDE_DIRS})
#### TODO
# It looks like here we will need to also include the src tree include 
# directory beacuse the directory structure of the headers in the 
# installation are all messed up once you attempt to include anything from include/private
include_directories(${MYSQLSRC_INCLUDE_DIRS}/include)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/sql)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/regex)

add_library(mosqlse SHARED ha_tapioca.cc tapioca_util.c  )
add_library(tapiocautil STATIC tapioca_util.c )

IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  SET_TARGET_PROPERTIES(mosqlse tapiocautil PROPERTIES COMPILE_FLAGS "-fPIC")
ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

target_link_libraries(mosqlse ${LIBTAPIOCA_LIBRARIES} ${MYSQL_LIBRARIES} 
	${LIBEVENT_LIBRARIES} ${MSGPACK_LIBRARIES} ${LIBUUID_LIBRARIES}
	${PCRE_LIBRARIES} )

set_property (TARGET mosqlse PROPERTY INSTALL_RPATH_USE_LINK_PATH TRUE)

INSTALL(TARGETS mosqlse DESTINATION lib)
#INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cm DESTINATION bin)
