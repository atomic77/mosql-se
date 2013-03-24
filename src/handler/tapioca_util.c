#include "tapioca_util.h"


/**
 * @brief
 * A utility function to create a string representation of an arbitrary
 * number of bytes at *m -- make sure *s has been allocated with
 * 3*num_bytes + 4 bytes of mem (eg. 0x <B0> <B1> ... where <Bn> is hex
 * representation of byte at position n)
 */
void memarea_as_string(char *s, unsigned char *m, int num_bytes) {
	int i;
	s[0] = '0';
	s[1] = 'x';
	s+= 2;
	for (i=0; i < num_bytes; i++) {
		sprintf(s, " %02x", *m);
		s+=3;
		m++;
	}
	s = '\0';
}



/**
 * Simple utility function to search bptree map for a name and return the id
 * Returns -1 if not found
 * FIXME Make use of HASH type provided by mysql, as this will be horribly slow
 * if the number of tables grows large
 */
// DEPRECATED
int16_t get_bptree_id_for_name(const char *table, const char *index,
		tapioca_bptree_info *bpt_map, int num_bptrees) {

	tapioca_bptree_info *ptr = bpt_map;
	int i;
	for (i = 0; i < num_bptrees; i++) {
/*
		printf("Checking %s | %s against %s | %s  \n",
				table, index, ptr->table_name, ptr->index_name);
		fflush(stdout);
*/
/*
		if (strcmp(ptr->table_name, table) == 0 &&
				strcmp(ptr->index_name, index) == 0 && ptr->is_active)
				return ptr->bpt_id;
*/
		ptr++;
	}
	return -1;

}
/**
 * Simple utility function to search bptree map for a name and return the id
 * Returns -1 if not found
 */
// DEPRECATED
int output_bptree_id_map(tapioca_bptree_info *bpt_map,	int num_bptrees) {
	int i;
	printf("bptree_id_map currently:\n");
	for (i = 0; i < num_bptrees; i++) {
	/*	printf("bptree id %d tablename %s idx %s is_pk %d active %d \n",
				bpt_map[i].bpt_id, bpt_map[i].table_name,
				bpt_map[i].index_name, bpt_map[i].is_pk, bpt_map[i].is_active);
		fflush(stdout);*/
	}
	return 1;

}

