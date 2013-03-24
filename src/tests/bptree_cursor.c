/**
 Test cases for functioning of cursor functions in b+tree
 like index_first, index_next, etc.

 Expected cursor behaviour:
 Table empty:

 index_first - rv 0
 index_next - rv 0 (but should never get called anyway)

 Table 1 row:
 index_first - rv 1
 following call to index_next should rv 0 and does not touch buffers

 Table 2+ rows:
 index_first - rv 1
 following call to index_next should rv 1
 */
#include "bptree_test_base.h"

int test_empty_bptree()
{
	int rv1, rv2;
	char k[BPTREE_VALUE_SIZE];
	char v[BPTREE_VALUE_SIZE];
	int ksize, vsize;

	tapioca_handle *th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, 1,
			BPTREE_OPEN_OVERWRITE);

	rv1 = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);

	tapioca_commit(th);
	tapioca_close(th);
	return (rv1 == 0);
}

int test_single_element_bptree()
{
	int rv1, rv2, rv;
	char k[5];
	char v[5];
	char kk[5] = "aaaa";
	char vv[5] = "cccc";
	int ksize, vsize = 5;

	tapioca_handle *th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, 1,
			BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	rv = tapioca_bptree_insert(th, tbpt_id, &kk, 5, &vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key %s\n", k);
	}

	tapioca_commit(th);

	rv1 = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);
	rv2 = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
	// Did tapioca_bptree_first fetch the correct values?
	rv = (memcmp(k, kk, 5) == 0) && (memcmp(v, vv, 5) == 0);
	return (rv1 == 1 && rv2 == 0 && rv);
}
int test_two_element_bptree()
{
	int rv1, rv2, rv3, rv;
	char k[5];
	char v[5];
	char kk[5] = "aaaa";
	char vv[5] = "cccc";
	int ksize, vsize = 5;
	bzero(k, 5);
	bzero(v, 5);

	tapioca_handle *th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, 1,
			BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	rv = tapioca_bptree_insert(th, tbpt_id, &kk, 5, &vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key %s\n", k);
	}
	tapioca_commit(th);
	kk[3] = 'b';
	vv[3] = 'd';
	tapioca_bptree_insert(th, tbpt_id, &kk, 5, &vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key %s\n", k);
	}
	tapioca_commit(th);

	rv1 = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);
	rv2 = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
	rv3 = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);

	// Did tapioca_bptree_next fetch the correct second value?
	rv = (memcmp(k, kk, 5) == 0) && (memcmp(v, vv, 5) == 0);
	return (rv1 == 1 && rv2 == 1 && rv3 == 0 && rv);
}

int test_partial_key_traversal(int keys)
{
	int rv1, rv2, rv3, rv, r, n, i;
	char k;
	char kk[10] = "aaaa";
	char vv[10] = "cccc";
	int ksize, vsize = 10;

	tapioca_handle *th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, 1,
			BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	int* arr;
	arr = malloc(keys * sizeof(int));
	for (i = 0; i < keys; i++)
		arr[i] = i;
	n = keys;
	int total_retries = 0;
	for (i = 0; i < keys; i++)
	{
		/*r = rand() % (n);
		 k = arr[r];
		 arr[r] = arr[n-1];
		 n--;*/
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", i);
		rv = -1;
		int attempts = 1;
		do
		{
			rv = tapioca_bptree_insert(th, tbpt_id, &kk, 10, &vv, 10,
					BPTREE_INSERT_UNIQUE_KEY);
			if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
			{
				printf("Tried to insert duplicate key %s\n", kk);
			}
			rv = tapioca_commit(th);
			if (rv != 0)
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

	/////////////////////////////////////////////////////////////////////////
	// Now we have a tree with some data; let's do a bunch of prefix searching
	int pkeys = (int) keys / 10;
	rv1 = rv2 = 1;
	for (i = 0; i < pkeys; i++)
	{
		char kpref[10];
		char vpref[10];
		char pref[10] = "a0000000";
		char *pptr = pref + 1;
		sprintf(pptr, "%07d", i);
		pref[8] = '\0';
		pref[9] = '\0';

		rv = tapioca_bptree_search(th, tbpt_id, pref, 10, vpref, &vsize);
		char comp1[10], comp2[10];
		memcpy(comp1, pref, 10);
		memcpy(comp2, pref, 10);
		comp1[8] = 0x30;
		comp2[8] = 0x31;
		tapioca_bptree_index_next(th, tbpt_id, kpref, &ksize, vpref, &vsize);
		rv1 = rv1 && memcmp(kpref, comp1, 10) == 0;
		tapioca_bptree_index_next(th, tbpt_id, kpref, &ksize, vpref, &vsize);
		rv2 = rv2 && memcmp(kpref, comp2, 10) == 0;
		if (!rv1 || !rv2)
		{
			int debugme = 12312;
		}
	}

	// search should have returned not found, and next two values
	// should be a020 and a021
	return (!rv && rv1 && rv2);
}

int main(int argc, char **argv)
{
	int rv, i, k; // , dbug, v, seed, num_threads;
	tapioca_handle *th;
	if (argc < 2)
	{
		printf("%s <Keys to insert> \n", argv[0]);
		exit(-1);
	}
	printf("Testing tapioca_bptree_first with empty bptree ..");
	rv = test_empty_bptree();
	printf("%s\n", rv ? "pass" : "fail");

	printf("Testing tapioca_bptree_first and next with single element bptree ... ");
	rv = test_single_element_bptree();
	printf("%s\n", rv ? "pass" : "fail");

	printf("Testing tapioca_bptree_first and next with two element bptree ... ");
	rv = test_two_element_bptree();
	printf("%s\n", rv ? "pass" : "fail");

	printf("Testing bptree partial key traversal ... ");
	fflush(stdout);
	rv = test_partial_key_traversal(atoi(argv[1]));
	printf("%s\n", rv ? "pass" : "fail");

	exit(0);
}
