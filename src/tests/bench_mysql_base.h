/*
    Copyright (C) 2013 University of Lugano

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


struct option long_options[] =
{
		/* These options set a flag. */
		{"test-type", 		required_argument, 0, 'y'},
		{"num-threads",   	required_argument, 0, 't'},
		{"num-operations",	required_argument,       0, 'n'},
		{"commit-rate",  	required_argument,       0, 'c'},
		{"storage-engine",  required_argument,       0, 's'},
		{"primary-key-parts",  required_argument,       0, 'p'},
		{"part-lengths",  required_argument,       0, 'q'},
		{"drop-recreate",  no_argument,       0, 'd'},
		{"start-key",  		required_argument,       0, 'k'},
		{"test-identifier",  required_argument,       0, 'i'},
		{"length-of-row",  required_argument,       0, 'l'},
		{"repair-after-create",  no_argument,       0, 'r'},
		{"host-name",  required_argument,       0, 'a'},
		{"port",  required_argument,       0, 'b'},
		{"help",  			no_argument,       0, 'h'},
		{0, 0, 0, 0}
};

const char *short_opts = "y:t:n:c:s:p:q:k:i:l:dra:b:h";

enum bench_test_type
{
	BENCH_INSERT_SEQUENTIAL=0,
	BENCH_INSERT_RANDOM_PK_IN_RANGE=1,
	BENCH_SELECT_RANDOM_PK_IN_RANGE=2,
	BENCH_UPDATE_RANDOM_PK_IN_RANGE=3,
	BENCH_INSERT_RANDOM_MULTIPART_PK_IN_RANGE=4,
	BENCH_SELECT_RANDOM_MULTIPART_PK_IN_RANGE=5,
	BENCH_UPDATE_RANDOM_MULTIPART_PK_IN_RANGE=6
};

// Struct with fields to be passed into various benchmarking threads
typedef struct bench_thread_data {
	MYSQL *conn;
	int n; // # of times test is called
	enum bench_test_type test_num;
	int thread_id;
	int commit_rate;
	int param1;
	int param2;
	int failures;
    struct timespec tm_st, tm_end, op_st, op_end;
	double druntime;
	uint32_t *response_times; // in microseconds
} bench_thread_data ;

const char *table_simple_one_pk =
   "CREATE TABLE t (t VARCHAR(4096), i int, primary key (i))";
const char *table_range_2 =
	"CREATE TABLE t (t VARCHAR(4096), i0 int, i1 int, primary key (i0,i1))";
const char *table_range_3 =
	"CREATE TABLE t (t VARCHAR(4096), i0 int, i1 int, i2 int, "
	"primary key (i0,i1,i2))";

void *bench_select_random_pk_in_range(void *data) ;
void *bench_update_random_pk_in_range(void *data);
void *bench_insert_random_pk_in_range(void *data) ;
void *bench_insert_sequential(void *data);
void *bench_insert_random_multipart_pk_in_range(void *data) ;
void *bench_select_random_multipart_pk_in_range(void *data) ;
void *bench_update_random_multipart_pk_in_range(void *data) ;


