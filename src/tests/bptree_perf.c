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

#include "bptree_test_base.h"

int commit_interval = 1;
int test_ordered_insert(int keys, int bpt_id)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2,fails = 0;
	char k[7] = "a00000";
	char v[7] = "v00000";
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, bpt_id, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY );
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 7, BPTREE_FIELD_COMP_STRNCMP);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 10000) % 10);
		k[2] = 0x30 + ((i / 1000) % 10);
		k[3] = 0x30 + ((i / 100) % 10);
		k[4] = 0x30 + ((i / 10) % 10);
		k[5] = 0x30 + (i % 10);
		if (i == 42) {
			int basdf= 0;
		}
		rv = tapioca_bptree_insert(th, tbpt_id, &k, 7, &v, 7);
		if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			printf("Tried to insert duplicate key %s\n", k);
		}
		if (i % commit_interval == 0)  {
			rv = tapioca_commit(th);
			if (rv < 0) fails++;
		}
		if (i % 1000 == 0)
			printf("Inserted %d keys\n", i);
	}
	rv = tapioca_commit(th);
	if (rv < 0) fails++;
	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("Insert completed in:           %.3f seconds, %.2f keys/s, "
			"%d cmtfails  \n",
			druntime, (double) (keys / druntime), fails);

	printf("Verifying ordering...OFF \n");
	clock_gettime(CLOCK_MONOTONIC, &tms);
//	rv1 = verify_bptree_order(bpt, bps, BPTREE_VERIFY_RECURSIVELY);
	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("Recursive scan completed in:   %.3f seconds, %.2f keys/s, rv %d \n",
			druntime, (double) (keys / druntime), rv1);

	clock_gettime(CLOCK_MONOTONIC, &tms);
//	rv2 = verify_bptree_order(bpt, bps, BPTREE_VERIFY_SEQUENTIALLY);
	clock_gettime(CLOCK_MONOTONIC, &tmend);

	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("Sequential scan completed in:  %.3f seconds, %.2f keys/s , rv %d\n",
			druntime, (double) (rv2 / druntime), rv2);
	//dump_bptree_contents(bpt, bps,1);
	free(th);
	return (rv1 && rv2);
}

int main(int argc, char **argv)
{
	int rv;
	if (argc < 4)
	{
		printf("%s <Keys to insert> <bptId> <insert commit interval>\n", argv[0]);
		exit(-1);
	}

	printf("Testing bptree insert then traverse... \n");
	commit_interval = atoi(argv[3]);
	rv = test_ordered_insert(atoi(argv[1]), atoi(argv[2]));
	printf("%s\n", rv ? "pass" : "fail");

}
