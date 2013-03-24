#include "bptree_test_base.h"

#define INSERT_MODE 0
#define SEARCH_MODE 1

int op_mode = 1;
tapioca_bptree_id bpt_id = 1;
int num_fields = 1;
int key_len = 0;
bptree_field *fields;
tapioca_handle *th;
tapioca_bptree_id tbpt_id;

int do_insert_op()
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2,fails = 0;
	size_t bytes_read = 1;
	char *instr = malloc(1);
	printf("Running insert tests. ");
	char *buf = malloc(key_len);
	char *bptr = buf;
	do
	{
		for(i = 0 ; i < num_fields; i++)
		{
			printf("Int value of field %d:", i);
			bytes_read = getline(&instr,&bytes_read, stdin);
			if (bytes_read == 1) {
				tapioca_close(th);
				return 1;
			}
			int val = atoi(instr);
			// FIXME Since we don't have the field structure we have to
			// maintain this locally
//			memcpy(bptr,&val,bps->bfield[i].f_sz);
//			bptr += bps->bfield[i].f_sz;

		}

		rv = tapioca_bptree_insert(th, tbpt_id, buf, key_len, &bytes_read, 4, BPTREE_INSERT_UNIQUE_KEY);
		printf("insert returned %d\n", rv);
		rv = tapioca_commit(th);
		printf("commit returned %d\n", rv);

	} while(bytes_read != 1);

	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key \n");
	}
	if (rv < 0) fails++;
	return rv;
}

int main(int argc, char **argv)
{
	int rv,i;
	size_t sz;
	if (argc < 3)
	{
		printf("%s <Operation> <bptId> <fields>\n", argv[0]);
		printf("Operation: 0 insert, 1, search\n");
		exit(-1);
	}
	bpt_id = atoi(argv[2]);
	num_fields = atoi(argv[3]);

	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, bpt_id, BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	tapioca_bptree_set_num_fields(th, tbpt_id, num_fields);
	for (i = 0; i < num_fields; i++)
	{
		printf("Field %d size:",i);
		char *instr = malloc(10);
		getline(&instr,&sz, stdin);
		int field_size = atoi(instr);
		switch (field_size) {
			case 1:
				tapioca_bptree_set_field_info(th, tbpt_id, i, field_size, BPTREE_FIELD_COMP_INT_8);
				break;
			case 2:
				tapioca_bptree_set_field_info(th, tbpt_id, i, field_size, BPTREE_FIELD_COMP_INT_16);
				break;
			case 4:
				tapioca_bptree_set_field_info(th, tbpt_id, i, field_size, BPTREE_FIELD_COMP_INT_32);
				break;
			default:
				tapioca_bptree_set_field_info(th, tbpt_id, i, field_size, BPTREE_FIELD_COMP_MEMCMP);
				break;
		}
		printf("Set field %d to size %d\n",i, field_size);
		key_len += field_size;
	}

	do_insert_op();

}
