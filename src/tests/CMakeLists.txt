include_directories(${LIBTAPIOCA_INCLUDE_DIRS})
include_directories(${MYSQLSRC_INCLUDE_DIRS}/include)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/sql)
include_directories(${MYSQLSRC_INCLUDE_DIRS}/regex)
include_directories(${LIBEVENT_INCLUDE_DIRS}/regex)

#IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
#  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
#ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

FILE(GLOB TEST_SOURCES *.c *.cc)

## TODO There is still a linking issue with some of the test cases, it may make
## more sense to separate out the 'utility' stuff into a separate directory and library
# and include this in both tests/ and handler/ and as part of a overall effort to 
# refactor the code 
foreach(p ${TEST_SOURCES})
	get_filename_component(target "${p}" NAME)
	add_executable(${target} ${p})
	target_link_libraries(${target}  ${LIBTAPIOCA_LIBRARIES} ${MYSQL_LIBRARIES} ${LIBEVENT_LIBRARIES} ${PROJECT_BINARY_DIR}/handler/libtapiocautil.a rt m c)
endforeach(p)

