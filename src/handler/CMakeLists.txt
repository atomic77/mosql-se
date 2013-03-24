#include_directories(${CMAKE_SOURCE_DIR}/db)
#include_directories(${CMAKE_SOURCE_DIR}/util)
#include_directories(${LIBEVENT_INCLUDE_DIRS})
#include_directories(${MSGPACK_INCLUDE_DIRS})
#include_directories(${LIBUUID_INCLUDE_DIRS})
include_directories(${LIBTAPIOCA_INCLUDE_DIRS})
#include_directories(${MYSQL_INCLUDE_DIRS})
#include_directories(${MYSQL_INCLUDE_DIRS})
#include_directories(${MYSQL_INCLUDE_DIRS}/..)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/include)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/sql)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/regex)
#### TODO
# It looks like here we will need to also include the src tree include directory beacuse the directory structure of the headers in the installation are all messed up once you attempt to include anything from include/private


#add_executable(tapioca_executable tapioca.c tcp.c  bplustree.c bplustree_client.c bplustree_debug.c btree.c tpl.c)
#SET_TARGET_PROPERTIES(tapioca_executable PROPERTIES OUTPUT_NAME "tapioca")

#target_link_libraries(tapioca_executable tapiocadb ${TAPIOCA_LINKER_LIBS}  ${LIBUUID_LIBRARIES} ${MSGPACK_LIBRARIES})

#GET_TARGET_PROPERTY(TAPIOCA_BIN tapioca_executable LOCATION)

#ADD_CUSTOM_COMMAND(TARGET tapioca_executable POST_BUILD
#          COMMAND ${CMAKE_COMMAND} -E copy ${TAPIOCA_BIN} ${PROJECT_BINARY_DIR}/bin)

#add_library(tapioca STATIC tcp_connection.c tcp_protocol.c 
#		libtapioca.c mget_result.c)
add_library(ha_mosql SHARED ha_tapioca.cc tapioca_util.c tpl.c )
add_library(tapiocautil STATIC tapioca_util.c tpl.c )

IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

target_link_libraries(ha_mosql ${LIBTAPIOCA_LIBRARIES} ${MYSQL_LIBRARIES})
