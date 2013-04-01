#include <tapioca/tapioca.h>
#include <tapioca/tapioca_btree.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>


#define TAPIOCA_ROW_PACKET_HEADER 0x3
#define BPTREE_META_NODE_PACKET_HEADER 0x4
#define BPTREE_NODE_PACKET_HEADER 0x5

int main(int argc, char **argv) {
	int rv,ksize,vsize,i, comm_sz = 0;
	int COMMIT_INTERVAL = 45000;
	int16_t bpt_id;
	int64_t node_id;
	tapioca_bptree_id tbpt_id;

	unsigned char k[65000], v[65000];
	int port;
	const char *tracefile;
	FILE *fp;
	fpos_t pos, tmp_pos;
	if (argc < 5)
	{
		printf("Usage %s <Host> <Port> <BptID> <Operation> \n", argv[0]);
		printf("Check enum bptree_debug_option for list of valid ops.\n");
		exit(-1);
	}
	port = atoi(argv[2]);
	tbpt_id = atoi(argv[3]);
	enum bptree_debug_option bdo = atoi(argv[4]);

	tapioca_handle *th = tapioca_open(argv[1],port);
	if (th == NULL) { printf("Tapioca conn failed\n"); exit(-1); }

	rv = tapioca_bptree_debug(th, tbpt_id, bdo);
	printf ("Got rv = %d from tapioca; you might have to check the server"
			" logs for more output.");
	exit(0);
}

