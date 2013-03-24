#include <my_global.h>
#include <mysql.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

static int thread_inserts = 50;

int insert_rows(MYSQL *conn, int thr_id, int row_count) {
	MYSQL_RES *result;
	MYSQL_ROW row;
	MYSQL_STMT *stmt;
	MYSQL_BIND param[2];
	int i, failed_commits = 0;
	const char *blah = "blah";
	stmt = mysql_stmt_init(conn);
	mysql_stmt_prepare(stmt,"INSERT INTO t VALUES (?,?)",26);
	for (i = (1+thr_id)*1000; i < (1+thr_id)*1000 + row_count; i++ ) {
//		sprintf(qrystr,"INSERT INTO t VALUES (%d,'%s');", i, "z");
//		printf("Thr %d query: %s \n",thr_id, qrystr);
		memset(param,0,sizeof(MYSQL_BIND)*2);
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &i;
		param[1].buffer_type = MYSQL_TYPE_STRING;
		param[1].buffer = blah;
		param[1].buffer_length = strlen(blah);
		if(mysql_stmt_bind_param(stmt, param)) return -1;
		if(mysql_stmt_execute(stmt)) return -1;
		if(mysql_commit(conn)) {
			if (mysql_errno(conn) == 1180) {
				failed_commits++;
			} else {
				return -1;
			}
		}
	}
	printf ("Thread %d had %d failed commits\n",thr_id,failed_commits);
	return(0);

}

int retrieve_count(MYSQL *conn, int thr_id) {
	MYSQL_RES *result;
	MYSQL_ROW row;
	if (mysql_query(conn, "select count(*) from t")) return -1;
	result = mysql_store_result(conn);
	if(mysql_num_fields(result) != 1) {
		printf("Expecting 1 column, got %d\n", mysql_num_fields(result));
		return -1;
	}
	row = mysql_fetch_row(result);

	printf("Thread %d got value %s for count\n",thr_id,row[0]);
	return(atoi(row[0]));

}
void *open_and_select(void *data) {

  MYSQL *conn;

  int *thr_id;
  thr_id = (int *)data;
  conn = mysql_init(NULL);

  if (conn == NULL) goto exception;

  if (mysql_real_connect(conn, "127.0.0.1", "root","","test1",3307,
          NULL, 0) == NULL) goto exception;

  printf("Thread %d connected to mysql \n",*thr_id);
  if(*thr_id % 2) {
	  // sleep for some random period b/w 0 and 3s

	  usleep((rand() % 5000) * 1000);
	  if(retrieve_count(conn, *thr_id) < 0) goto exception;
  } else {
	  mysql_autocommit(conn,0);
	  if(insert_rows(conn, *thr_id, thread_inserts) < 0) goto exception;
  }

  mysql_close(conn);
  return;

exception:
 printf("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
 exit(-1);

}

int main(int argc, char **argv)
{
	pthread_t *threads;
	int *thr_data;
	int num_threads, base_inserts,i,rv;

	srand(0);
	if (argc < 4) {
		printf("%s <num threads> <base inserts> <thread inserts>\n", argv[0]);
		exit(-1);
	}
	num_threads = atoi(argv[1]);
	base_inserts = atoi(argv[2]);
	thread_inserts = atoi(argv[3]);
	printf("Preparing test table with %d rows\n", base_inserts);
	MYSQL *conn;
	MYSQL_ROW row;
	conn = mysql_init(NULL);

	if (conn == NULL) goto exception;
	if (mysql_real_connect(conn, "127.0.0.1", "root","","test",3307,
			NULL, 0) == NULL) goto exception;
	if (mysql_query(conn, "DROP SCHEMA IF EXISTS test1")) goto exception;
	if (mysql_query(conn, "CREATE SCHEMA test1")) goto exception;
	mysql_select_db(conn, "test1");
	if (mysql_query(conn, "create table t (i int, t varchar(30), primary key (i)) engine = tapioca;")) goto exception;
	char ins[256] ;

	for(i = 1; i< base_inserts; i++) {
		sprintf(ins,"INSERT INTO t VALUES (%d,'%s')",i,"base");
		if (mysql_query(conn, ins)) goto exception;
	}
//	usleep(500*1000);

	printf("Launching threads\n");

	threads = malloc(sizeof(pthread_t)*num_threads);
	thr_data = malloc(sizeof(int)*num_threads);

	for (i = 0; i < num_threads; i++) {
		thr_data[i] = i;
		rv = pthread_create(&(threads[i]), NULL, open_and_select, &thr_data[i]);
	}
	for (i = 0; i < num_threads; i++) {
		rv = pthread_join(threads[i], NULL);
	}

	free(threads);
	exit(0);
exception:
	printf("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
	exit(-1);

}
