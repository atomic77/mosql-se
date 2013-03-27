//#include "bptree.h"
#include <tapioca.h>
#include <stdio.h>

// FIXME These names are now not correct, but legacy references still exist
#define TAPIOCA_MAX_VALUE_BUFFER 64000
#define TAPIOCA_LARGE_BUFFER 60000 // stay a bit under 64k
#define TAPIOCA_POOL_SIZE 10

// Important metadata keys we store in tapioca
#define TAPIOCA_TABLE_AUTOINC_KEY "TAPIOCA_TABLE_AUTOINC"
#define TAPIOCA_BPTREE_AUTOINC_KEY "TAPIOCA_BPTREE_AUTOINC"
#define TAPIOCA_BPTREE_META_KEY "TAPIOCA_BPTREE_MAP"

#define TAPIOCA_ROW_PACKET_HEADER 0x3

#define TAPIOCA_MGET_BUFFER_SIZE 9

// As per http://dev.mysql.com/doc/refman/5.1/en/identifiers.html
// but i have no idea where this constant is defined...
#define TAPIOCA_MAX_TABLE_NAME_LEN 64

#define TAPIOCA_TPL_BPTREE_ID_MAP_FMT "A(S(jc#jj))"

typedef struct tapioca_bptree_info {
	tapioca_bptree_id bpt_id;
//	char index_name[TAPIOCA_MAX_TABLE_NAME_LEN];
	char full_index_name[TAPIOCA_MAX_TABLE_NAME_LEN*2];
	int16_t is_pk;
	int16_t is_active;
} tapioca_bptree_info;


void memarea_as_string(char *s, unsigned char *m, int num_bytes);
int16_t get_bptree_id_for_name(const char *table, const char *index,
		tapioca_bptree_info *bpt_map,int num_bptrees);
int output_bptree_id_map(tapioca_bptree_info *bpt_map,	int num_bptrees) ;
