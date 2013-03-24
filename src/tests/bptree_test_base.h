/**

 Base test methods used in some of the test programs
 */
//#include "bptree.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <tapioca.h>
#include <stdio.h>

typedef struct serialize_struct
{
	tapioca_bptree_id seed;
	int dbug;
	int keys;
	int thread_id;
	int start_key;
} serialize_struct;


/*
int verify_bptree_order(bptree_handle *bpt, bptree_session *bps,
		enum bptree_order_verify mode);

int output_bptree(bptree_handle *bpt, bptree_session *bps);
int bptree_sequential_read(bptree_handle *bpt, bptree_session *bps, int binary);
*/
