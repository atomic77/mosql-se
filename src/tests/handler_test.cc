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

/**
 * Testing for some of the utility functions in the handler code
 */
extern "C"
{
//#include "bptree.h"
#include "tapioca.h"
#include "tapioca_util.h"
#include <string.h>
#include <stdio.h>

}

int test_corrupt_bptree_node_unmarshall()
{

}
int test_corrupt_bptree_node_marshall()
{

}
int test_null_bptree_node_unmarshall()
{
	size_t bsize1, bsize2;
	int c;
/*	Test with a zeroed out, invalid and null buffer;
	We will do tpl_peek to ensure that the buffer is valid, but if we have
	passed in a buffer that we are not allowed to touch we will still
	fail here; nevertheless the protections in place now are a big
	improvement from before...*/
/*	unsigned char buf_zero[BPTREE_VALUE_SIZE];
	unsigned char buf_uninit[BPTREE_VALUE_SIZE];
	unsigned char *buf_null;
	bzero(buf_zero, BPTREE_VALUE_SIZE);
	bptree_node *n1, *n2, *n3;
	n1 = unmarshall_bptree_node(buf_zero, BPTREE_VALUE_SIZE);
	n2 = unmarshall_bptree_node(buf_uninit, BPTREE_VALUE_SIZE);
	n3 = unmarshall_bptree_node(buf_null, BPTREE_VALUE_SIZE);

	return (n1 == NULL && n2 == NULL && n3 == NULL);*/
	return 0;
}
int test_null_bptree_node_marshall()
{
/*	size_t bsize1, bsize2;
	int c;
	unsigned char *buf, *buf2;
	bptree_node n, n_noinit, *n_null;
	n.key_count = 2;
	n.self_key = 1;
	n.leaf = 1;
	n.children[0] = 1;
	n.children[1] = 1;
	n.children[2] = 1;
	n.key_sizes[0] = 5;
	n.key_sizes[1] = 7;
	n.keys[0] = (unsigned char*) malloc(5);
	n.keys[1] = (unsigned char*) malloc(7);
	;
	n.keys[0] = (unsigned char*) "aaaa";
	n.keys[1] = (unsigned char*) "aaaaaa";
	n.value_sizes[0] = 3;
	n.value_sizes[1] = 9;
	n.values[0] = (unsigned char*) malloc(3);
	n.values[1] = (unsigned char*) malloc(9);
	n.values[0] = (unsigned char*) "ccc";
	n.values[1] = (unsigned char*) "dddddddd";
	// first is an invalid node; n_null is a null ptr
	buf = (unsigned char*) marshall_bptree_node(&n_noinit, &bsize1);
	buf2 = (unsigned char*) marshall_bptree_node(n_null, &bsize2);
	return (buf == NULL && buf2 == NULL);*/
	return 0;
}

int test_bptree_node_serialization()
{
	unsigned char buf[1024];
	bzero(buf, 1024);
	uint16_t i;
	//	tapioca_bptree_info *m = unmarshall_bptree_id_map(buf, &i);
	//	return m == NULL;
}
int main(int argc, char **argv)
{
	int rv, i, k; // , dbug, v, seed, num_threads;
/*	bptree_handle *bpt;

	printf("Testing marshalling null bptree nodes... ");
	rv = test_null_bptree_node_marshall();
	printf("%s\n", rv ? "pass" : "FAIL");
	printf("Testing unmarshalling null bptree nodes... ");
	rv = test_null_bptree_node_unmarshall();
	printf("%s\n", rv ? "pass" : "FAIL");

	exit(0);*/
}
