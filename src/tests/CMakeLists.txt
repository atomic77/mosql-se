include_directories(${LIBTAPIOCA_INCLUDE_DIRS})
include_directories(${MYSQLSRC_INCLUDE_DIRS}/include)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/sql)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/regex)

#IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
#  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
#ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

#FILE(GLOB TEST_SOURCES *.c *.cc)

# Fix the naming convention of files in here (or move finally everything to 
# google test framework) so that we don't have to manually specify what to build

#SET (TEST_SOURCES bench_mysql_base.c  bptree_concurrent.c  bptree_dump.c bptree_perf.c tapioca_bptree_check.c tapioca_trace_recover.c bptree_basic.c bptree_cursor.c bptree_op_test.c tapioca_storage.c test_mysql_multi_open.c)
SET (TEST_SOURCES    bptree_dump.c bptree_perf.c tapioca_bptree_check.c tapioca_trace_recover.c )

## TODO There is still a linking issue with some of the test cases, it may make
## more sense to separate out the 'utility' stuff into a separate directory and library
# and include this in both tests/ and handler/ and as part of a overall effort to 
# refactor the code 
foreach(p ${TEST_SOURCES})
	get_filename_component(target "${p}" NAME_WE)
	add_executable(${target} ${p})
	target_link_libraries(${target}  ${LIBTAPIOCA_LIBRARIES} ${MYSQL_LIBRARIES} ${LIBEVENT_LIBRARIES} ${PROJECT_BINARY_DIR}/handler/libtapiocautil.a rt m c pthread)
endforeach(p)

