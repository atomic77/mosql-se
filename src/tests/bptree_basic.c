/**

 * Adapted from integer-based internal btree from tapioca sources
 *
 * This is a b+tree implementation that operates entirely external to tapioca
 * cluster and uses the basic get/put/commit API to provide bptree functionality
 * Arbitrary binary data can be indexed provided that a pointer to a function
 * is provided for ordering (similar to qsort())
 */
#include "bptree_test_base.h"

/*int test_serialization()
{
	size_t bsize1, bsize2;
	int c;
	void *buf, *buf2;
	tapioca_bptree_node n, *n2;
	n.key_count = 2;
	n.self_key = 1;
	n.leaf = 1;
	n.children[0] = 1;
	n.children[1] = 1;
	n.children[2] = 1;
	n.key_sizes[0] = 5;
	n.key_sizes[1] = 7;
	n.keys[0] = malloc(5);
	n.keys[1] = malloc(7);
	;
	n.keys[0] = "aaaa";
	n.keys[1] = "aaaaaa";
	n.value_sizes[0] = 3;
	n.value_sizes[1] = 9;
	n.values[0] = malloc(3);
	n.values[1] = malloc(9);
	n.values[0] = "ccc";
	n.values[1] = "dddddddd";
	buf = marshall_tapioca_bptree_node(&n, &bsize1);
	n2 = unmarshall_tapioca_bptree_node(buf, bsize1);
	buf2 = marshall_tapioca_bptree_node(n2, &bsize2);
	c = memcmp(buf, buf2, bsize1);
	// The buffers buf and buf2 should now be identical
	return (bsize1 > 0 && bsize1 == bsize2 && c == 0);

}*/

int test_update(int keys)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v000";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
		if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			printf("Tried to insert duplicate key %s\n", k);
		}
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}

	char v2[10] = "abcd12345";
	for (i = 1; i <= keys / 2; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		tapioca_bptree_update(th, tbpt_id, k, 5, v2, 10);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Updated %d keys\n", i);
	}

	printf("Verifying ordering...OFF ");
/*	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
	rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
	dump_tapioca_bptree_contents(th, tbpt_id, 1, 0);*/
	return (rv1 && rv2);
}
int test_multi_field_insert_dupe(int keys)
{
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int a, b, c, d;
	a = b = c = d = 1;
	unsigned char k[16];
	unsigned char v[4];
	memcpy(k, &a, 4);
	memcpy(k + 4, &b, 4);
	memcpy(k + 8, &c, 4);
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 2;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 1;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_close(th);
	return 1;
}

int test_multi_field_insert_dupe2(int keys)
{
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int a, b, c, d;
	a = b = c = d = 1;
	unsigned char k[16];
	unsigned char v[4];
	memcpy(k, &a, 4);
	memcpy(k + 4, &b, 4);
	memcpy(k + 8, &c, 4);
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 2;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 1;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_close(th);
	return 1;
}

int test_multi_field_insert(int keys)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v000";
	char kbuf[18];
	char *kptr;
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		kptr = kbuf;
		memcpy(kptr, k, 5);
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		rv = tapioca_bptree_insert(th, tbpt_id, kbuf, 18, v, 5, BPTREE_INSERT_UNIQUE_KEY);
		if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			printf("Tried to insert duplicate key %s\n", k);
		}
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}

	char v2[10] = "abcd12345";
	for (i = 1; i <= keys / 2; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		kptr = kbuf;
		memcpy(kptr, k, 5);
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		tapioca_bptree_update(th, tbpt_id, kbuf, 5, v2, 10);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Updated %d keys\n", i);
	}

	printf("Verifying ordering... OFF");
//	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
//	rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
//	dump_tapioca_bptree_contents(th, tbpt_id, 1, 1);
	return (rv1 && rv2);
}
int test_insert_dupe(int keys)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v000";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}
	// insert the last key we just inserted

	rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key %s\n", k);
	}
	return rv;
}
int test_mget(int keys)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v100";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open(address, port);
	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_put(th, &k, 5, &v, 5);
		tapioca_commit(th);
		if (i % 250 == 0) printf("Put %d keys\n", i);
	}


	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_mget(th, &k, 5);
	}

	mget_result *mres = tapioca_mget_commit(th);
	int mgets = 0;
	char vv[5];
	while(mget_result_count(mres) > 0)
	{
		int bytes = mget_result_consume(mres, vv);
		//printf("Mget sz %d Val %d : %s\n", bytes, mgets, vv);
		mgets++;
	}
	mget_result_free(mres);
	return rv;
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
	int keys = atoi(argv[1]);
	int mgets = keys > 10 ? 10 : keys;
	printf("Testing mget interface with %d keys...", mgets);
	rv = test_mget(mgets);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing multi-field insert...\n");
	rv = test_multi_field_insert(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing multi-field insert dupe...");
	rv = test_multi_field_insert_dupe(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

/*	printf("Testing serialization ... ");
	rv = test_serialization();
	printf("%s\n", rv ? "pass" : "FAIL");*/

	printf("Testing insert / update... ");
	rv = test_update(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing insert duplicate...");
	rv = test_insert_dupe(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	exit(0);
}
