#include <my_global.h>
#include <mysql.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>

#include "bench_mysql_base.h"

// Global stuff that will get set by the command line params
static int num_threads;
static int thread_ops;
static int drop_recreate = 0;
static int repair_after_create = 0;
static int start_key = 0;
static char *test_identifier;
static char *host;
static int port; // length of the character field we'll insert
static int row_len = 4; // length of the character field we'll insert
static char *cdata;
static int num_pk_parts = 1;
static int part_lengths[6]; // max of 6


void base_pk_insert_update(bench_thread_data *btd, const char *qry, int is_random,
		int pk_size, int *pk_parts);
int *convert_to_multipart(int r, int *part_sizes, int num_parts);
int32_t * init_sample_set(int32_t n, int32_t start_key);

void track_thread_start(bench_thread_data *btd)
{
	clock_gettime(CLOCK_MONOTONIC, &btd->tm_st);
}

void calc_thread_runtime(bench_thread_data *btd)
{
	clock_gettime(CLOCK_MONOTONIC, &btd->tm_end);
	long int runtime = (btd->tm_end.tv_nsec - btd->tm_st.tv_nsec);
	btd->druntime = (btd->tm_end.tv_sec - btd->tm_st.tv_sec) + (runtime
			/ 1000000000.0);
}

void track_op_start(bench_thread_data *btd)
{
	clock_gettime(CLOCK_MONOTONIC, &btd->op_st);
}

// 1 second = 1 000 milliseconds
// 1 second = 1 000 000 microseconds
// 1 second = 1 000 000 000 nanoseconds
void calc_op_runtime(bench_thread_data *btd, int i)
{
	clock_gettime(CLOCK_MONOTONIC, &btd->op_end);
	time_t sec = (btd->op_end.tv_sec - btd->op_st.tv_sec);
	long int nsec = (btd->op_end.tv_nsec - btd->op_st.tv_nsec);
	btd->response_times[i] = (nsec / 1000) + sec * 1000 * 1000;
	//(uint32_t)(btd->op_end.tv_sec - btd->op_st.tv_sec)*1000*1000;
	//memcpy(btd->response_times[*i], &r, sizeof(uint32_t));

}

/* Initialize an array to be used for sampling random numbers
 */
int32_t * init_sample_set(int32_t n, int32_t start_key)
{
	int32_t i;
	int32_t * arr = malloc(n * sizeof(int32_t));
	for (i = 0; i < n; i++)
		arr[i] = i + start_key;
	return arr;
}

/* Sample a random number from *arr; n is automatically
 * decremented to reflect the new size of the set
 * Returns -1 when empty
 */
int32_t sample_from_set(int32_t *arr, int *n)
{
	if (*n <= 0)
		return -1;
	int32_t k;
	int32_t r = rand() % (*n);
	k = arr[r];
	arr[r] = arr[*n - 1];
	(*n)--;
	return k;
}

int retrieve_count(MYSQL *conn, int thr_id)
{
	MYSQL_RES *result;
	MYSQL_ROW row;
	if (mysql_query(conn, "select count(*) from t"))
		return -1;
	result = mysql_store_result(conn);
	if (mysql_num_fields(result) != 1)
	{
		fprintf(stderr, "Expecting 1 column, got %d\n",
				mysql_num_fields(result));
		return -1;
	}
	row = mysql_fetch_row(result);

	fprintf(stderr, "Thread %d got value %s for count\n", thr_id, row[0]);
	return (atoi(row[0]));
}

void *bench_select_random_multipart_pk_in_range(void *data)
{
	bench_thread_data *btd = (bench_thread_data *) data;
	bench_select_random_pk_in_range(data);
	return NULL;
}

void *bench_update_random_multipart_pk_in_range(void *data)
{
	bench_thread_data *btd = (bench_thread_data *) data;
	int i;
	char qry[256] = "UPDATE t SET t = ? WHERE i0 = ? ";

	// We want this to be a multi-row update, so use one less column
	for (i = 1; i < num_pk_parts - 1; i++)
	{
		char *sect;
		asprintf(&sect, " AND i%d = ? ", i);
		strcat(qry, sect);
	}
	strcat(qry, " ; ");
	//	fprintf(stderr, "Preparing UPDATE statement %s \n",qry);
	base_pk_insert_update(btd, qry, 1, num_pk_parts - 1, part_lengths);
	return NULL;
}

void *bench_insert_random_multipart_pk_in_range(void *data)
{
	bench_thread_data *btd = (bench_thread_data *) data;
	int i;
	char qry[256] = "INSERT INTO t VALUES (?\0";
	for (i = 0; i < num_pk_parts; i++)
	{
		strcat(qry, ",?");
	}
	strcat(qry, ");");

	//	fprintf(stderr, "Preparing INSERT statement %s \n",qry);
	base_pk_insert_update(btd, qry, 1, num_pk_parts, part_lengths);
	return NULL;
}

/*
 * param1 - start key
 * param2 - end key
 * param3 - ?
 */
void *bench_insert_random_pk_in_range(void *data)
{
	bench_thread_data *btd = (bench_thread_data *) data;
	const char *qry = "INSERT INTO t VALUES (?,?)\0";
	base_pk_insert_update(btd, qry, 0, 1, NULL);
	return NULL;
}

/*
 * param1 - start key
 * param2 - end key
 */
void *bench_insert_sequential(void *data)
{

	bench_thread_data *btd = (bench_thread_data *) data;
	const char *qry = "INSERT INTO t VALUES (?,?)\0";
	base_pk_insert_update(btd, qry, 1, 1, NULL);
	return NULL;

}

void *bench_update_random_pk_in_range(void *data)
{
	bench_thread_data *btd = (bench_thread_data *) data;
	const char *qry = "UPDATE t SET t = ? WHERE i = ?\0";
	base_pk_insert_update(btd, qry, 1, 1, NULL);
	return NULL;
}

int *convert_to_multipart(int r, int *part_sizes, int num_parts)
{
	int i;
	if (num_parts < 1)
		return NULL;

	int *p = malloc(sizeof(int) * num_parts);

	if (num_parts == 1)
	{
		*p = r;
		return p;
	}
	// Hack off any front bits that may be greater than the first part
	int r2 = ((r >> part_sizes[0]) << part_sizes[0]) ^ r;
	for (i = 0; i < num_parts - 1; i++)
	{
		p[i] = r2 >> part_sizes[i + 1];
		r2 = (p[i] << part_sizes[i + 1]) ^ r2;
	}
	p[num_parts - 1] = r2;
	return p;

}
void base_pk_insert_update(bench_thread_data *btd, const char *qry, int is_random,
		int pk_size, int *pk_parts)
{

	MYSQL *conn = btd->conn;
	MYSQL_RES *result;
	MYSQL_ROW row;
	MYSQL_STMT *stmt;
	MYSQL_BIND *param = malloc(sizeof(MYSQL_BIND) * (1 + pk_size));
	int i, j, p, failed_commits = 0, n;
	//const char *txtdata = "blah";
	//  if (btd->cdata != NULL)
	//	  txtdata = btd->cdata;
	int start = btd->param1;
	int end = btd->param2;
	n = btd->n;
	int32_t *samples;
	samples = init_sample_set(n, start);

	if (btd->commit_rate == 0)
	{
		mysql_autocommit(conn, 1);
	}
	else
	{
		mysql_autocommit(conn, 0);
	}
	stmt = mysql_stmt_init(conn);
	mysql_select_db(conn, "test1");
	mysql_stmt_prepare(stmt, qry, strlen(qry));

	fprintf(stderr, "Thread %d starting insert/update range %d - %d \n",
			btd->thread_id, start, end);
	track_thread_start(btd);
	j = 0;
	for (i = start; i < end; i++)
	{
		int32_t *pk;
		int32_t r;
		if (btd->test_num == 1)
		{
			r = sample_from_set(samples, &n);
		}
		else
		{
			r = i;
		}

		// This function will not do much if using a single-part pk
		pk = convert_to_multipart(r, pk_parts, pk_size);

		//fprintf(stderr, "THD %d r=%d Writing PK %d - %d\n", btd->thread_id,
		// r,pk[0],pk[1]);
		MYSQL_BIND *pptr = param;
		memset(pptr, 0, sizeof(MYSQL_BIND));
		pptr->buffer_type = MYSQL_TYPE_STRING;
		pptr->buffer = cdata;
		pptr->buffer_length = row_len;
		pptr++;
		for (p = 1; p <= pk_size; p++)
		{
			memset(pptr, 0, sizeof(MYSQL_BIND));
			pptr->buffer_type = MYSQL_TYPE_LONG;
			pptr->buffer = pk;
			pptr++;
			pk++;
		}
		if (mysql_stmt_bind_param(stmt, param))
			goto exception;
		track_op_start(btd);
		if (mysql_stmt_execute(stmt))
			// 1180 -- 'Generic' error, 1637 - too many concurrent txn
			if (mysql_errno(conn) == 1180 || mysql_errno(conn) == 1637)
			{
				failed_commits++;
			}
			else
			{
				goto exception;
			}

		if (!btd->commit_rate)
			continue;

		if (!(i % btd->commit_rate))
			if (mysql_commit(conn))
				if (mysql_errno(conn) == 1180 || mysql_errno(conn) == 1637)
				{
					failed_commits++;
				}
				else
				{
					goto exception;
				}

		calc_op_runtime(btd, j);
		j++;
	}
	calc_thread_runtime(btd);
	btd->failures = failed_commits;
	fprintf(stderr, "Thread %d had %d failed commits\n", btd->thread_id,
			failed_commits);

	return;

	exception: fprintf(stderr, "Error %u: %s\n", mysql_errno(conn),
			mysql_error(conn));
	exit(-1);

}

/*
 * param1 - start key
 * param2 - end key
 * param3 - ?
 */
void *bench_select_random_pk_in_range(void *data)
{

	bench_thread_data *btd = (bench_thread_data *) data;

	MYSQL *conn = btd->conn;
	MYSQL_RES *result;
	MYSQL_ROW row;
	MYSQL_STMT *stmt;
	MYSQL_BIND param[1];
	MYSQL_BIND column[2];
	int32_t c_i;
	char c_t[1024];
	int i, j, failed_commits = 0, n;
	const char *txtdata = "blah";
	int *samples;
	btd->failures = 0;

	mysql_autocommit(conn, 1);
	stmt = mysql_stmt_init(conn);
	mysql_select_db(conn, "test1");
	switch (num_pk_parts)
	{
	case 1:
		mysql_stmt_prepare(stmt, "SELECT SQL_NO_CACHE i,t FROM t WHERE i = ?",
				29 + 13);
		break;
	case 2:
		mysql_stmt_prepare(stmt,
				"SELECT SQL_NO_CACHE i0, concat('T',count(*)) as t FROM t"
				" WHERE i0 = ? GROUP BY 1",
				67 + 13);
		break;
	case 3:
		mysql_stmt_prepare(stmt,
				"SELECT SQL_NO_CACHE i0,concat('T',count(*)) as t FROM t "
				"WHERE i0 = ? AND i1 = ? GROUP BY 1",
				77 + 13);
		break;
	default:
		fprintf(stderr, "Unsupported number of pk parts\n");
		return NULL;
	}

	int start = btd->param1;
	int end = btd->param2;
	n = btd->n;

	samples = init_sample_set(n, start);

	fprintf(stderr, "Thread %d starting selects in range %d %d \n",
			btd->thread_id, btd->param1, btd->param2);
	track_thread_start(btd);
	j = 0;
	for (i = start; i < end; i++)
	{
		int32_t pk = sample_from_set(samples, &n);
		memset(param, 0, sizeof(MYSQL_BIND) * 1);
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &pk;
		if (mysql_stmt_bind_param(stmt, param))
			goto exception;
		track_op_start(btd);
		if (mysql_stmt_execute(stmt))
			goto exception;
		calc_op_runtime(btd, j);
		//if(mysql_stmt_store_result(stmt)) goto exception;
		memset(column, 0, sizeof(MYSQL_BIND) * 2); /* initialize */
		column[0].buffer_type = MYSQL_TYPE_LONG;
		column[0].buffer = &c_i;
		column[1].buffer_type = MYSQL_TYPE_STRING;
		column[1].buffer = c_t;
		column[1].buffer_length = sizeof(c_t);
		if (mysql_stmt_bind_result(stmt, column))
			goto exception;
		switch (mysql_stmt_fetch(stmt))
		{
		case 0: //SUCCESS
		case MYSQL_DATA_TRUNCATED:
			break;
		case MYSQL_NO_DATA: //NO MORE DATA
			//fprintf(stderr, "Row not found for PK %d\n", pk);
			btd->failures++;
			break;
		case 1: //ERROR
		default:
			mysql_stmt_free_result(stmt);
			goto exception;
		}
		j++;

	}
	calc_thread_runtime(btd);

	fprintf(stderr, "Thread %d had %d failed lookups\n", btd->thread_id,
			btd->failures);

	// I don't know why this is complaining about return NULL
	return NULL;

	exception: fprintf(stderr, "Error %u: %s\n", mysql_errno(conn),
			mysql_error(conn));
	fprintf(stderr, "Stmt Error %u: %s\n", mysql_stmt_errno(stmt),
			mysql_stmt_error(stmt));
	exit(-1);

}

// Prepares a table with given sql by dropping and recreating the schema
int prep_tables(enum bench_test_type test_num, char *storage_engine)
{

	if (drop_recreate)
	{
		switch (test_num)
		{
		// For update/select tests assume the table is in place
		case BENCH_SELECT_RANDOM_PK_IN_RANGE:
		case BENCH_SELECT_RANDOM_MULTIPART_PK_IN_RANGE:
		case BENCH_UPDATE_RANDOM_MULTIPART_PK_IN_RANGE:
		case BENCH_UPDATE_RANDOM_PK_IN_RANGE:
			return 0;
		case BENCH_INSERT_RANDOM_PK_IN_RANGE:
		case BENCH_INSERT_SEQUENTIAL:
			return (recreate_table_in_schema(table_simple_one_pk,
					storage_engine));
		case BENCH_INSERT_RANDOM_MULTIPART_PK_IN_RANGE:
			if (num_pk_parts == 2)
			{
				return(recreate_table_in_schema(table_range_2, storage_engine));
			}
			else if (num_pk_parts == 3)
			{
				return(recreate_table_in_schema(table_range_3, storage_engine));
			}
			else
			{
				return -1;
			}
			// This should be made flexible as well
			// return(recreate_table_in_schema(table_range_2, storage_engine));
		default:
			fprintf(stderr, "Unsupported test type %d !\n", test_num);
			return -1;
		}
	}
	else
	{
		return 0;
	}
}

int recreate_table_in_schema(char *table_sql, char *storage_engine)
{
	fprintf(stderr, "Preparing test table \n");
	MYSQL *conn;
	MYSQL_ROW row;
	conn = mysql_init(NULL);

	if (conn == NULL) return -1;
	if (mysql_real_connect(conn, host,"tpcc","tpcc", "", port, NULL, 0) == NULL)
		return -1;

	mysql_query(conn, "DROP SCHEMA IF EXISTS test1");
	mysql_query(conn, "DROP SCHEMA IF EXISTS test1");
	if (mysql_query(conn, "CREATE SCHEMA test1")) goto exception;
	mysql_select_db(conn, "test1");
	char set_engine[128];
	sprintf(set_engine, "SET storage_engine=%s", storage_engine);
	if (mysql_query(conn, set_engine)) goto exception;
	if (mysql_query(conn, table_sql)) goto exception;

	// If we are using this in a n node mysql configuration, we need to ensure
	// REPAIR TABLE is run on nodes 2 .. n so that metadata is picked up from
	// tapioca
	if (repair_after_create)
		if (mysql_query(conn, "REPAIR TABLE t;"))
			goto exception;

	mysql_close(conn);
	return 0;
	exception: fprintf(stderr, "Error %u: %s\n", mysql_errno(conn),
			mysql_error(conn));
	mysql_close(conn);
	return -1;
}

void print_usage()
{

	struct option opt = long_options[0];
	int i = 0;
	fprintf(stderr, "Command line options:\n");
	while (1)
	{
		if (opt.name == 0)
			break;
		fprintf(stderr, "\t--%s , -%c \n", opt.name, opt.val);
		i++;
		opt = long_options[i];
	}
	fprintf(stderr,
			"See enum bench_test_type in header file for test numbers\n");
}
/* Tab-sep format:
 * TestIdentifier, ThdId, TestId, StartKey, EndKey, Elapsed, Rate, Aborts,
 * Response Min, Resp .05, R med, R .95, R Max
 */
int output_thread_statistics(bench_thread_data *btd)
{
	int i;
	uint32_t rmin, rpct5, rmed, rpct95, rmax;

	for (i = 0; i < num_threads; i++)
	{
		//		fprintf(stdout, "Thread %d Test Num %d Start/End %d %d finished"
		// 		" in %.3f s, %.2f n/s \n",
		//				btd[i].thread_id, btd[i].test_num, btd[i].param1 ,
		//				btd[i].param2,	btd[i].druntime, rate);

		float rate = (float) btd[i].n / btd[i].druntime;
		fprintf(stdout, "%s\t", test_identifier);
		fprintf(stdout, "%d\t%d\t%d\t", btd[i].thread_id, btd[i].test_num,
				btd[i].param1);
		fprintf(stdout, "%d\t%.3f\t%.1f\t", btd[i].param2, btd[i].druntime,
				rate);
		fprintf(stdout, "%d\t", btd[i].failures);
		calc_histogram_stats(btd[i].response_times, thread_ops, &rmin, &rpct5,
				&rmed, &rpct95, &rmax);
		fprintf(stdout,"%d\t%d\t%d\t%d\t%d\n", rmin, rpct5, rmed, rpct95, rmax);
	}
	return 1;
}

int uint32_t_cmp(const void *a, const void *b)
{
	const uint32_t *i = (const uint32_t *) a;
	const uint32_t *j = (const uint32_t *) b;
	return *i - *j;
}

int calc_histogram_stats(uint32_t *response_times, int n, uint32_t *rmin,
		uint32_t *rpct5, uint32_t *rmed, uint32_t *rpct95, uint32_t *rmax)
{
	// Assume the times are not sorted;
	qsort(response_times, n, sizeof(uint32_t), uint32_t_cmp);
	*rmin = response_times[0];
	*rpct5 = response_times[(int) floor(n * 0.05)];
	*rmed = response_times[(int) floor(n * 0.5)];
	*rpct95 = response_times[(int) floor(n * 0.95)];
	*rmax = response_times[n - 2];
	return 1;
}

int main(int argc, char **argv)
{
	pthread_t *threads;
	int test_num, base_inserts, i, rv, commit_rate;//,thread_ops;
	char storage_engine[16];
	const char *delim = ",";
	srand(0);
	bench_thread_data *btd;

	void *(*bench_proc[7])(void *data);
	bench_proc[BENCH_INSERT_SEQUENTIAL] = bench_insert_sequential;
	bench_proc[BENCH_INSERT_RANDOM_PK_IN_RANGE]
			= bench_insert_random_pk_in_range;
	bench_proc[BENCH_SELECT_RANDOM_PK_IN_RANGE]
			= bench_select_random_pk_in_range;
	bench_proc[BENCH_UPDATE_RANDOM_PK_IN_RANGE]
			= bench_update_random_pk_in_range;
	bench_proc[BENCH_INSERT_RANDOM_MULTIPART_PK_IN_RANGE]
			= bench_insert_random_multipart_pk_in_range;
	bench_proc[BENCH_SELECT_RANDOM_MULTIPART_PK_IN_RANGE]
			= bench_select_random_multipart_pk_in_range;
	bench_proc[BENCH_UPDATE_RANDOM_MULTIPART_PK_IN_RANGE]
			= bench_update_random_multipart_pk_in_range;

	int opt_idx;
	char ch;
	test_num = -1;
	while ((ch = getopt_long(argc, argv, short_opts, long_options, &opt_idx))
			!= -1)
	{
		switch (ch)
		{
		case 'y':
			test_num = atoi(optarg);
			break;
		case 't':
			num_threads = atoi(optarg);
			break;
		case 'n':
			thread_ops = atoi(optarg);
			break;
		case 'c':
			commit_rate = atoi(optarg);
			break;
		case 'q':
			i = 0;
			char *tok = strtok(optarg, delim);
			while (tok != NULL)
			{
				part_lengths[i] = atoi(tok);
				tok = strtok(NULL, delim);
				i++;
			}
			break;
		case 'p':
			num_pk_parts = atoi(optarg);
			break;
		case 'k':
			start_key = atoi(optarg);
			break;
		case 'd':
			drop_recreate = 1;
			break;
		case 'i':
			asprintf(&test_identifier, "%s", optarg);
			break;
		case 'r':
			repair_after_create = 1;
			break;
		case 'l':
			row_len = atoi(optarg);
			break;
		case 's':
			strcpy(storage_engine, optarg);
			break;
		case 'a':
			asprintf(&host, "%s", optarg);
			break;
		case 'b':
			port = atoi(optarg);
			break;
		default:
		case 'h':
			print_usage();
			exit(1);
		}
	}
	if (argc < 2 || test_num == -1)
	{
		print_usage();
		exit(1);
	}

	if (prep_tables(test_num, storage_engine))
	{
		fprintf(stderr, "Could not prepare tables\n");
		exit(-1);
	}

	if (num_pk_parts > 1)
	{
		thread_ops = 1 << part_lengths[0];
		thread_ops = thread_ops / num_threads;
		fprintf(stderr, "Overriding thread ops to %d based on pk part sizes\n",
				thread_ops);
	}
	float mb_est = (thread_ops * num_threads * 4) / (1024.0f);
	float data_size_est = (thread_ops * num_threads * (row_len + 6))
			/ (1024.0f);
	fprintf(stderr,
			"Approx. mem use for this test: %.2f KB, data written: %.2f KB\n",
			mb_est, data_size_est);

	cdata = malloc(row_len);
	char c = 0x61;
	memset(cdata, c, row_len);

	threads = malloc(sizeof(pthread_t) * num_threads);
	btd = malloc(sizeof(bench_thread_data) * num_threads);

	// preallocate memory and connectoins to avoid delays in launching threads
	fprintf(stderr, "Allocating memory and mysql connections\n");
	for (i = 0; i < num_threads; i++)
	{
		btd[i].response_times = malloc(sizeof(uint32_t) * thread_ops);
		btd[i].conn = mysql_init(NULL);
		if (mysql_real_connect(btd[i].conn, host, "tpcc", "tpcc", "",
				port, NULL, 0) == NULL)
		{
			fprintf(stderr,
				"Connection for thread %d could not be established! %d %s \n",
					i, mysql_errno(btd[i].conn), mysql_error(btd[i].conn));
			return -1;
		}
	}

	fprintf(stderr, "Launching threads\n");
	for (i = 0; i < num_threads; i++)
	{
		btd[i].thread_id = i;
		btd[i].param1 = start_key + i * thread_ops;
		btd[i].param2 = start_key + (i + 1) * thread_ops;
		btd[i].n = thread_ops;
		btd[i].test_num = test_num;
		btd[i].commit_rate = commit_rate;
		//		btd[i].response_times = malloc(sizeof(uint32_t)*thread_ops);
		rv = pthread_create(&(threads[i]), NULL, bench_proc[test_num], &btd[i]);
	}

	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	fprintf(stderr, "Tests complete. Post-processing results\n");
	output_thread_statistics(btd);

	free(threads);
	exit(0);
}
