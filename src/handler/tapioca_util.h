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

//#include "bptree.h"
#include <tapioca/tapioca.h>
#include <tapioca/tapioca_btree.h>
#include <stdio.h>

// FIXME These names are now not correct, but legacy references still exist
#define TAPIOCA_MAX_VALUE_SIZE 64000
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

struct tapioca_bptree_info {
	tapioca_bptree_id bpt_id;
//	char index_name[TAPIOCA_MAX_TABLE_NAME_LEN];
	char full_index_name[TAPIOCA_MAX_TABLE_NAME_LEN*2];
	int16_t is_pk;
	int16_t is_active;
} ;


void memarea_as_string(char *s, unsigned char *m, int num_bytes);
int16_t get_bptree_id_for_name(const char *table, const char *index,
		struct tapioca_bptree_info *bpt_map,int num_bptrees);
int output_bptree_id_map(struct tapioca_bptree_info *bpt_map,	int num_bptrees) ;
