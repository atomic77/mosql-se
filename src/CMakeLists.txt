cmake_minimum_required(VERSION 2.6)
project(mosql-se)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

find_package(LIBTAPIOCA REQUIRED)
find_package(MYSQLSRC REQUIRED)
find_package(MYSQL REQUIRED)
#find_package(LIBPAXOS REQUIRED)
#find_package(BDB REQUIRED)
find_package(LIBEVENT REQUIRED)
#find_package(LIBPAXOS REQUIRED)
#find_package(LIBUUID REQUIRED)
#find_package(MSGPACK REQUIRED)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing -fno-rtti -DMYSQL_DYNAMIC_PLUGIN")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing -DMYSQL_DYNAMIC_PLUGIN")

# SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")
# SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")
# INCLUDE("${PROJECT_SOURCE_DIR}/storage/mysql_storage_engine.cmake")
#SET(EXAMPLE_SOURCES ha_example.cc)
#MYSQL_STORAGE_ENGINE(EXAMPLE)

# More stuff from CSV storage engine:

#SET(CSV_SOURCES  ha_tina.cc ha_tina.h transparent_file.cc transparent_file.h)
#MYSQL_ADD_PLUGIN(csv ${CSV_SOURCES} STORAGE_ENGINE MANDATORY)

add_subdirectory(handler)
add_subdirectory(tests)

INSTALL(FILES ../scripts/launch_db.sh DESTINATION scripts)
INSTALL(FILES ../config/my.cnf DESTINATION config)