#include "tapioca.h"
#include <stdio.h>

int main(int argc, char **argv)
{
	int rv, i, k; // , dbug, v, seed, num_threads;
	tapioca_handle *th;
	enum bptree_debug_option debug_opt;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;

	if (argc < 2)
	{
		printf("%s <bpt_id> <debug opt>\n", argv[0]);
		printf("Debug options (from bplustree_client.h):\n"
		"\tBPTREE_DEBUG_VERIFY_SEQUENTIALLY=0,\n"
		"\tBPTREE_DEBUG_VERIFY_RECURSIVELY,  /* Traverse tree recursively */\n"
		"\tBPTREE_DEBUG_DUMP_SEQUENTIAL, /* _next() based dump of tree */\n"
		"\tBPTREE_DEBUG_DUMP_RECURSIVE /* recursive traversal of tree */\n");

		exit(1);
	}

	th = tapioca_open("127.0.0.1", 5555);
	if (th == NULL)
	{
		printf("Couldn't connect to tapioca\n");
		exit(-1);
	}
	tbpt_id = atoi(argv[1]);
	debug_opt = atoi(argv[2]);
	printf("Dumping bptree %d; contents will appear in server log!\n", tbpt_id);

	rv = tapioca_bptree_debug(th, tbpt_id,debug_opt);
	printf("Got %d from server\n",rv);
}
