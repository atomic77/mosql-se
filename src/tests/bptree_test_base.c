/**

 Base test methods used in some of the test programs
 */
#include "bptree_test_base.h"
#include "../handler/tapioca_util.h"
#include <stdio.h>
#include <math.h>


/*int bptree_sequential_read_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node *n, int binary);
int output_bptree_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node* n, int level, int binary);*/
int dump_bptree_contents(tapioca_handle *th, tapioca_bptree_id tbpt_id, int dbug,
		int binary);

int verify_bptree_sequential_read(tapioca_handle *th, tapioca_bptree_id tbpt_id);

/*int verify_bptree_order_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node *n, char *largest);*/

/*
int verify_bptree_order(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		enum bptree_order_verify mode)
{
	int rv, i;
	bptree_node *root;
	bptree_meta_node *bpm;

	bpm = read_meta_node(bpt, bps);
	if (bpm == NULL)
		return -1;

	root = read_node(bpt, bps, bpm->root_key);
	if (root == NULL)
		return -1;
	if (mode == BPTREE_VERIFY_RECURSIVELY)
	{
		char k[100];
		bzero(k, 100);
		rv = verify_bptree_order_recursive(bpt, bps, root, k);
		//bptree_commit(bpt);
	}
	else if (mode == BPTREE_VERIFY_SEQUENTIALLY)
	{
		rv = verify_bptree_sequential_read(bpt, bps);
	}
	bptree_commit(bpt);
	free_node(&root);
	return rv;
}
*/

int verify_bptree_sequential_read(tapioca_handle *th, tapioca_bptree_id tbpt_id)
{
	int ksize, vsize,rv;
	char kcur[100], kprev[100], v[100];
	int elements = 1;

	rv = tapioca_bptree_index_first(th, tbpt_id, kprev, &ksize, v, &vsize);
	if (rv <= 0 ) return 0;
	while (tapioca_bptree_index_next(th, tbpt_id, kcur, &ksize, v, &vsize) > 0)
	{
		elements++;
		//intf("%s at cell id %lld pos %d elems %d \n", kcur, bps->cursor_cell_id,
//		 bps->cursor_pos, elements);
		if (memcmp(kprev, kcur,ksize) > 0)
		{
			printf("B+Tree validation failed, %s > %s\n", kprev, kcur);
			//output_bptree(bpt);
			return 0;
		}
		memcpy(kprev, kcur, 100);
	}
	return elements;

}
/*
int verify_bptree_order_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node *n, char *largest)
{
	int c, rv;
	if (!is_cell_ordered(bps, n))
		return -1;
	largest = n->keys[n->key_count - 1];
	if (!n->leaf)
	{
		// To make this easier just use the first 100 bytes
		char kcur[100], kprev[100];
		memset(kcur, '\0', 100);
		memset(kprev, '\0', 100);
		for (c = 0; c <= n->key_count; c++)
		{
			bptree_node *next;
			next = read_node(bpt, bps, n->children[c]);
			if (next == NULL) return 0;
			rv = verify_bptree_order_recursive(bpt, bps, next, kcur);
			if (bptree_compar(bps, kprev, kcur) > 0) return 0;
			memcpy(kprev, kcur, 100);
			free_node(&next);
			if (!rv) return rv;
		}
		return 1;
	}
	return 1;
}
*/
/*

int output_bptree(tapioca_handle *th, tapioca_bptree_id *tbpt_id, int binary)
{
	int rv, i;
	bptree_node *root;
	bptree_meta_node *bpm;

	bpm = read_meta_node(bpt, bps);
	if (bpm == NULL)
		return -1;

	root = read_node(bpt, bps, bpm->root_key);
	if (root == NULL)
		return -1;

	printf("digraph BTree { \n");
	rv = output_bptree_recursive(bpt, bps, root, 0, binary);
	printf("}\n");
}

int output_bptree_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node* n, int level, int binary)
{
	int k, c;
	assert (n->key_count <= BPTREE_NODE_SIZE);
	printf("\n\n");
	printf("N%lld [ label = < ", n->self_key);
	for (k = 0; k < n->key_count; k++)
	{
		if(!binary)
		{
			printf("%s, ", n->keys[k]);
		} else
		{
			char c[30];
			int sz = (int)fmin(10, n->key_sizes[k]);
			memarea_as_string(c, n->keys[k],sz);
			printf("%s, ", c);
		}
	}
	printf(" > ] \n");
	if (!n->leaf)
	{
		for (c = 0; c <= n->key_count; c++)
		{
			printf("N%lld -> N%lld\n ", n->self_key, n->children[c]);
		}
		for (c = 0; c <= n->key_count; c++)
		{
			bptree_node *next;
			next = read_node(bpt, bps, n->children[c]);
			if (next == NULL)
				return -1;
			output_bptree_recursive(bpt, bps, next, level + 1, binary);
		}
	}
	else
	{
		 We should print these fwd/back links for correctness, but it makes
		 * the output too hard to visualize
		if (n->next_node > 0)
			printf("N%lld -> N%lld\n ", n->self_key, n->next_node);
		if (n->prev_node > 0)
			printf("N%lld -> N%lld\n ", n->self_key, n->prev_node);

	}
}
int bptree_sequential_read(tapioca_handle *th, tapioca_bptree_id *tbpt_id, int binary)
{
	int rv, i;
	bptree_node *root;
	bptree_meta_node *bpm;

	bpm = read_meta_node(bpt, bps);
	if (bpm == NULL)
		return -1;

	root = read_node(bpt, bps, bpm->root_key);
	if (root == NULL)
		return -1;

	return bptree_sequential_read_recursive(bpt, bps, root, binary);
}

*
 * For testing purposes, read out the elements in the B+Tree sequentially
 * to ensure ordering

int bptree_sequential_read_recursive(tapioca_handle *th, tapioca_bptree_id *tbpt_id,
		bptree_node *n, int binary)
{
	int k, c;
	if (!n->leaf)
	{
		for (c = 0; c <= n->key_count; c++)
		{
			bptree_node *next;
			if ((next = read_node(bpt, bps, n->children[c])) == NULL)
				return -1;

			bptree_sequential_read_recursive(bpt, bps, next, binary);
		}
	}
	else
	{
		for (k = 0; k < n->key_count; k++)
		{
			char kstr[50];
			char vstr[50];
			if (binary)
			{
				memarea_as_string(kstr, n->keys[k], fmin(50, n->key_sizes[k]));
				memarea_as_string(vstr, n->values[k],
						fmin(50, n->value_sizes[k]));
				printf("Key:%s  Value %s at cell id %lld pos %d \n", kstr,
						vstr, n->self_key, k);
			}
			else
			{
				printf("%s\n", n->keys[k]);
			}
		}
	}
}
*/

int dump_bptree_contents(tapioca_handle *th, tapioca_bptree_id tbpt_id, int dbug,
		int binary)
{
	char k2[BPTREE_VALUE_SIZE], v2[BPTREE_VALUE_SIZE];
	int ksize, vsize;
	if (dbug)
	{
		printf("Recursive sequential read:\n");
//		bptree_sequential_read(th, tbpt_id, binary);
		printf("Leaf-based sequential read:\n");

		tapioca_commit(th);
		tapioca_bptree_index_first(th, tbpt_id, k2, &ksize, v2, &vsize);

		do
		{
			char kstr[50];
			char vstr[50];
			if (binary)
			{
				memarea_as_string(kstr, k2, fmin(50, ksize));
				memarea_as_string(vstr, v2, fmin(50, vsize));
				printf("Key:%s  Value %s at cell id %lld pos %d \n", kstr);
			}
			else
			{
				printf("Key:%s Value %s at cell id %lld pos %d \n", k2, v2);
			}
		} while (tapioca_bptree_index_next(th, tbpt_id, k2, &ksize, v2, &vsize) > 0);
	}
	if (dbug > 1)
	{
		printf("-----------------------\n");
//		output_bptree(th, tbpt_id, 0);
	}
}

