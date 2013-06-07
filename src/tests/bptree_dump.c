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

#include <tapioca/tapioca.h>
#include <tapioca/tapioca_btree.h>
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
