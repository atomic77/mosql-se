#include "bptree_test_base.h"

void *tapioca_bptree_traversal_test(void *data)
{
	// Assume that we've inserted k keys into index; pick a random place to
	// search and then verify that we index_next the correct # of records
	int i, j, k, v, n, r, rv, ksize, vsize;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	serialize_struct *s = (serialize_struct *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}

	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	char k2[10];
	char v2[10];
	arr = malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;

	printf("THR_ID %d: Searching then traversing keys...\n", s->thread_id);
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		int cnt = 0;
		do
		{
			rv = tapioca_bptree_index_next(th, tbpt_id, k2, &ksize, v2, &vsize);
			if (memcmp(k2, kk, 10) < 0)
			{
				printf("Index_next return out-of-order element: k2, kk %s %s\n",
						k2, kk);
				return 0;
			}
			cnt++;
		} while (rv == 1);

		if (cnt < s->keys - k)
		{
			//printf ("Index_next returned %d keys, expecting %d\n",
			//	cnt, (s->keys -k));
		}
		//if (rv !=1) printf("THR_ID %d: Failed to find %s!\n",s->thread_id,kk);
		tapioca_commit(th);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f traversals/s \n",
			s->thread_id, druntime, (double) (s->keys / druntime));

	/// Test index scanning

	//assert(verify_tapioca_bptree_order(th,1));
	//tapioca_commit(th);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}

void *tapioca_bptree_search_test(void *data)
{
	int i, j, k, v, n, r, rv, vsize;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	serialize_struct *s = (serialize_struct *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}

	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	arr = malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;

	printf("THR_ID %d: Searching keys... starting from %d \n", s->thread_id,
			s->start_key);
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		//if (rv !=1) printf("THR_ID %d: Failed to find %s!\n",s->thread_id,kk);
		tapioca_commit(th);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f searches/s \n",
			s->thread_id, druntime, (double) (s->keys / druntime));

	/// Test index scanning

	//assert(verify_tapioca_bptree_order(th,1));
	//tapioca_commit(th);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}
void *tapioca_bptree_insert_test(void *data)
{
	int i, j, k, v, n, r, rv;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	serialize_struct *s = (serialize_struct *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	if (s->thread_id == 0)
	{
		th = tapioca_open("127.0.0.1", 5555);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_OVERWRITE);
	}
	else
	{
		th = tapioca_open("127.0.0.1", 5555);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	}
	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	printf("THR_ID %d connected to node id %d", s->thread_id,
			tapioca_node_id(th));
	arr = malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	printf("THR_ID %d: Writing keys starting from %d...\n", s->thread_id,
			s->start_key);
	clock_gettime(CLOCK_MONOTONIC, &tms);
	int total_retries = 0;
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = -1;
		int attempts = 1;
		do
		{
			printf("Inserting k/v %s / %s \n", kk, vv);
			rv = tapioca_bptree_insert(th, tbpt_id, kk, 10, vv, 10,
					BPTREE_INSERT_UNIQUE_KEY);
			//if (rv != tapioca_bptree_ERR_DUPLICATE_KEY_INSERTED)
			// rv here could be DUPLICATE_KEY; do we care?
			if (rv < 0) {
				printf("Error on insert; will wait a bit\n");
				long wait = 5 * 100 * 1000 + (rand() % 100) * 1000;
				attempts++;
				usleep(wait);
			}
			else if (rv != BPTREE_ERR_DUPLICATE_KEY_INSERTED)
			{
				rv = tapioca_commit(th);
			}
			else
			{
				long wait = 100 * 1000 + (rand() % 100) * 1000;
				attempts++;
				usleep(wait);
			}

		} while (rv != 0 && attempts < 10);
		if (rv != 0)
		{
			printf("Could  not commit %s after %d tries\n", kk, attempts);
		}
		total_retries += attempts;

		tapioca_commit(th);
	}
	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f ins/s %d total tries \n",
			s->thread_id, druntime, (double) (s->keys / druntime),
			total_retries);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}

void *tapioca_bptree_serialization_test(void *data) // so we can instantiate as a thread
{
	int i, j, k, v, n, r, rv;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	serialize_struct *s = (serialize_struct *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	if (s->thread_id == 0)
	{
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_initialize_bpt_session(th, s->seed, BPTREE_OPEN_OVERWRITE);
	}
	else
	{
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	}
	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}
	return NULL;

}

void *tapioca_bptree_test_concurrent_access(void *data)
{ // so that we can instantiate as a thread
	int rv, i, j, k, v;
	serialize_struct *s = (serialize_struct *) data;
	tapioca_handle *th;
	th = tapioca_open("127.0.0.1", 5555);
	k = 1;
	v = s->thread_id;
	printf("THR_ID %d start \n", s->thread_id);
	fflush(stdout);
	if (!s->thread_id == 0)
		sleep(5);

	rv = tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
	rv = tapioca_get(th, &k, sizeof(int), &v, sizeof(int));

	printf("THR_ID %d pre-commit k/v:  %d / %d \n", s->thread_id, k, v);
	fflush(stdout);
	if (s->thread_id == 0)
		sleep(10);
	rv = tapioca_commit(th);
	printf("THR_ID %d commit rv:  %d\n", s->thread_id, rv);
	fflush(stdout);

	rv = tapioca_get(th, &k, sizeof(int), &v, sizeof(int));
	printf("THR_ID %d post-commit k/v:  %d / %d \n", s->thread_id, k, v);
	fflush(stdout);
	rv = tapioca_commit(th);
	return NULL;
}

int test_insert_and_traverse(int argc, char **argv)
{
	int rv, i, k, dbug, v, seed, num_threads;
	struct timespec tms, tmend;
	long runtime;
	double druntime;
	tapioca_handle *th;
	pthread_t *threads;
	serialize_struct *s;

	k = atoi(argv[1]);
	dbug = atoi(argv[2]);
	seed = atoi(argv[3]);
	num_threads = atoi(argv[4]);
	srand(seed);

	threads = malloc(sizeof(pthread_t) * num_threads);

	s = malloc(sizeof(serialize_struct) * num_threads * 2);

	// Pre-insert
	for (i = 0; i < num_threads; i++)
	{
		s[i].start_key = i * k;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = k;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, tapioca_bptree_insert_test, &s[i]);
		if (i == 0)
			usleep(500 * 1000); // Let the first thd destroy the existing b+tree
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	free(threads);
	threads = malloc(sizeof(pthread_t) * num_threads * 2);

	// Do concurrent traversal for starting keys, then insert more keys after
	// in parallel
	printf("Launching parallel insert/search\n");
	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_create(&(threads[i]), NULL, tapioca_bptree_traversal_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = num_threads; i < num_threads * 2; i++)
	{
		s[i].start_key = i * k;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = k;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, tapioca_bptree_insert_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = 0; i < num_threads * 2; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th,tbpt_id,dbug,0);
	}
	free(threads);
	return 1;
}

int test_insert_and_search(int argc, char **argv)
{
	int rv, i, k, dbug, v, seed, num_threads;
	struct timespec tms, tmend;
	long runtime;
	double druntime;
	tapioca_handle *th;
	pthread_t *ins_threads;
	pthread_t *srch_threads;
	serialize_struct *ins;
	serialize_struct *srch;

	k = atoi(argv[1]);
	dbug = atoi(argv[2]);
	seed = atoi(argv[3]);
	num_threads = atoi(argv[4]);
	srand(seed);

	ins_threads = malloc(sizeof(pthread_t) * num_threads);
	srch_threads = malloc(sizeof(pthread_t) * num_threads);

	ins = malloc(sizeof(serialize_struct) * num_threads);
	srch = malloc(sizeof(serialize_struct) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < num_threads; i++)
	{
		ins[i].start_key = i * k;
		ins[i].seed = srch[i].seed = seed;
		ins[i].dbug = srch[i].dbug = dbug;
		ins[i].keys = srch[i].keys = k;
		ins[i].thread_id = i;
		srch[i].thread_id = num_threads + i;
		srch[i].start_key = (num_threads - i) * k;
		rv = pthread_create(&(ins_threads[i]), NULL, tapioca_bptree_insert_test,
				&ins[i]);
		if (i == 0)
			usleep(500 * 1000); // Let the first thd destroy the existing b+tree
		// we can share the s struct cause nothing gets changed
		rv = pthread_create(&(srch_threads[i]), NULL, tapioca_bptree_search_test,
				&srch[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_join(srch_threads[i], NULL);
		rv = pthread_join(ins_threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nInserts completed in %.3f seconds, %.2f ins/s \n\n",
			druntime, ((k * num_threads) / druntime));

	free(ins_threads);
	free(srch_threads);

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, seed,
				BPTREE_OPEN_ONLY);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	return 1;
}
int test_insert_then_search(int argc, char **argv)
{
	int rv, i, k, dbug, v, seed, num_threads;
	struct timespec tms, tmend;
	long runtime;
	double druntime;
	tapioca_handle *th;
	pthread_t *threads;
	serialize_struct *s;

	k = atoi(argv[1]);
	dbug = atoi(argv[2]);
	seed = atoi(argv[3]);
	num_threads = atoi(argv[4]);
	srand(seed);

	threads = malloc(sizeof(pthread_t) * num_threads);

	s = malloc(sizeof(serialize_struct) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < num_threads; i++)
	{
		s[i].start_key = i * k;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = k;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, tapioca_bptree_insert_test, &s[i]);
		if (i == 0)
			usleep(500 * 1000); // Let first thread destroy the existing b+tree
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nInserts completed in %.3f seconds, %.2f ins/s \n\n",
			druntime, ((k * num_threads) / druntime));

	free(threads);
	threads = malloc(sizeof(pthread_t) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_create(&(threads[i]), NULL, tapioca_bptree_search_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nSearches completed in %.3f seconds, %.2f searches/s \n",
			druntime, ((k * num_threads) / druntime));

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	free(threads);
	return 1;
}

int main(int argc, char **argv)
{

	if (argc < 5)
	{
		printf("%s <Key to insert> <print tree> <seed/bptid> <num threads> \n",
				argv[0]);
		exit(-1);
	}

	int rv;
	/*
	printf("===============================================================\n");
	printf("Testing bptree insert then search... ");
	rv = test_insert_then_search(argc, argv);
	printf("%s\n", rv ? "pass" : "fail");
	 */

	printf("===============================================================\n");
	printf("Testing bptree insert then concurrent insert/traverse... ");
	rv = test_insert_and_traverse(argc, argv);
	printf("%s\n", rv ? "pass" : "fail");

	/*
	printf("===============================================================\n");
	printf("Testing bptree concurrent insert and search... ");
	rv = test_insert_and_search(argc, argv);
	printf("%s\n", rv ? "pass" : "fail");
	printf("===============================================================\n");
	 */


	exit(0);
}
