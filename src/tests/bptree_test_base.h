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

 Base test methods used in some of the test programs
 */
//#include "bptree.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <tapioca/tapioca.h>
#include <tapioca/tapioca_btree.h>
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
