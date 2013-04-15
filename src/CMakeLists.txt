cmake_minimum_required(VERSION 2.6)
project(mosql-se)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

find_package(LIBTAPIOCA REQUIRED)
find_package(MYSQLSRC REQUIRED)
find_package(MYSQL REQUIRED)
find_package(LIBEVENT REQUIRED)

#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing -fno-rtti -DMYSQL_DYNAMIC_PLUGIN")
#SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing -DMYSQL_DYNAMIC_PLUGIN")

SET (MYSQLCXXFLAGS "-mtune=native -DUNIV_MUST_NOT_INLINE -fno-exceptions -fno-rtti -DMYSQL_DYNAMIC_PLUGIN -rdynamic")
SET (MYSQLCFLAGS "-mtune=native -DUNIV_MUST_NOT_INLINE -fno-exceptions -DMYSQL_DYNAMIC_PLUGIN -rdynamic")
SET (MYDEBUGFLAGS " -O0 -g3 -DEXTRA_DEBUG -DSAFEMALLOC -DPEDANTIC_SAFEMALLOC -DFORCE_INIT_OF_VARS  -DSAFE_MUTEX ")
SET (MYOPTFLAGS " -g -O2 ")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MYSQLCXXFLAGS} ${MYDEBUGFLAGS}")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MYSQLCFLAGS} ${MYDEBUGFLAGS}")


add_subdirectory(handler)
add_subdirectory(tests)

INSTALL(FILES ../scripts/launch_db.sh DESTINATION scripts)
INSTALL(FILES ../config/my.cnf DESTINATION config)