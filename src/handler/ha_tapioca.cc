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
 @file ha_tapioca.cc
 @brief
 @details
 @note
 @code
 @endcode
 */
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "ha_tapioca.h"

// Files used during repair and update
static handler* tapioca_create_handler(handlerton *hton, TABLE_SHARE *table,
		MEM_ROOT *mem_root);

/************ Storage Engine Globals ********************/

handlerton *tapioca_hton;

/* Variables for tapioca share methods */

/*
 Hash used to track the number of open tables; variable for tapioca share
 methods
 */
static HASH tapioca_open_tables;
/* The mutex used to init the hash; variable for tapioca share methods */
pthread_mutex_t tapioca_mutex;

static HASH tapioca_bptrees;
static tapioca_handle *th_global; // a bpt handle used for admin/metadata
bool th_global_enabled = false;

// Arbitrarily decide that we can use up to 8 nodes per mysql instance
static tapioca_node_config tapioca_nodes[TAPIOCA_MAX_NODES_PER_MYSQL];
static int num_tapioca_nodes = -1;
static int32_t conn_counter = 0;

// a sequence maintained at the storage engine level so we don't have to use
// tapioca to get unique execution_ids for the B+Tree
// TODO Makes more sense to use the administrative connection to maintain this
static volatile uint32_t local_execution_id = 1;

static int reload_tapioca_bptree_metadata(uint16_t *num_bptrees_read);
static int tapioca_parse_config();
bool is_thrloc_sane(tapioca_thrloc *thrloc);

/************ Storage Engine Globals ********************/

uint32_t get_next_bptree_execution_id(int node)
{
	uint32_t inst_num = tapioca_nodes[node].mysql_instance_num;
	return inst_num << 24 | ++local_execution_id;
}
/**
 @brief
 Function we use in the creation of our hash to get key.
 */
static uchar*
tapioca_get_key(TAPIOCA_SHARE *share, size_t *length,
				my_bool not_used __attribute__((unused)))
{
	*length = share->table_name_length;
	return (uchar*) share->table_name;
}

// Function we use in our hash of open tables/indexes in a thread
static uchar*
tsession_get_key(tapioca_table_session *tsession, size_t *length,
		my_bool not_used __attribute__((unused)))
{
	*length = strlen(tsession->full_table_name);
	return (uchar*) tsession->full_table_name;
}

// Function we use in our hash of open tables in a thread
static uchar* tapioca_bptree_info_get_key(struct tapioca_bptree_info *bpt_info,
		size_t *length, my_bool not_used __attribute__((unused)))
{
	*length = strlen(bpt_info->full_index_name);
	return (uchar*) bpt_info->full_index_name;
}

/** We will keep around a global storage-engine level connection to tapioca
 * for things like metadata management. This MUST be called within a mutex to
 * the appropriate global structures! Uses b+tree id 0
 */
static int init_administrative_connection()
{
	DBUG_ENTER("init_administrative_connection");
	DBUG_PRINT("ha_tapioca", ("Opening administrative bptree connection"));
	th_global = tapioca_open(tapioca_nodes[0].address, tapioca_nodes[0].port);
	if (th_global == NULL) DBUG_RETURN(-1);
	th_global_enabled = true;
	DBUG_RETURN(0);
}
int ha_tapioca_commit(handlerton *hton, THD *thd, bool all)
{
	DBUG_ENTER("ha_tapioca_commit");
	int rv;
	tapioca_thrloc *thrloc;
	thrloc = (tapioca_thrloc *) thd_get_ha_data(thd, tapioca_hton);
	if (thrloc == NULL)
	{
		printf("TAPIOCA: Failed to lookup thread loc in ha_tapioca_commit\n");
		DBUG_RETURN(-1);
	}
	assert(is_thrloc_sane(thrloc));
	assert(thrloc->tx_active); // we shouldn't get in here if the tx is inactive
	if (thrloc->tx_cant_commit) {
		tapioca_rollback(thrloc->th);
		thrloc->tx_active = false;
		thrloc->tx_cant_commit = false;
		DBUG_RETURN( HA_ERR_TOO_MANY_CONCURRENT_TRXS);
	}
	
	rv = tapioca_commit(thrloc->th);
	
	DBUG_PRINT("ha_tapioca",
			("Commit in ha_tapioca_commit tid %lu bpt* %p rv %d \n",
					thd->real_id, thrloc->th, rv));
	thrloc->tx_active = false;
	if (rv < 0)
		DBUG_RETURN( HA_ERR_TOO_MANY_CONCURRENT_TRXS);

	statistic_add(thd->status_var.ha_savepoint_count, rv, &LOCK_status);
	statistic_increment( thd->status_var.ha_savepoint_rollback_count, &LOCK_status);
	DBUG_RETURN(0);
}
int ha_tapioca_rollback(handlerton *hton, THD *thd, bool all)
{
	DBUG_ENTER("ha_tapioca_rollback");
	DBUG_PRINT("ha_tapioca",
		("Entered ha_tapioca_rollback in thd_id %lu \n", thd->real_id));
	tapioca_thrloc *thrloc;
	thrloc = (tapioca_thrloc *) thd_get_ha_data(thd, tapioca_hton);
	if (thrloc == NULL)
		DBUG_RETURN(-1);
	assert(is_thrloc_sane(thrloc));
	assert(thrloc->tx_active); // we shouldn't get in here if the tx is inactive
	
	tapioca_rollback(thrloc->th);
	
	thrloc->tx_active = false;
	thrloc->tx_cant_commit = false;
	DBUG_RETURN(0);
}

static int tapioca_init_func(void *p)
{
	DBUG_ENTER("tapioca_init_func");
	tapioca_hton = (handlerton *) p;
	int rv;
	srand(time(NULL));

	pthread_mutex_init(&tapioca_mutex, MY_MUTEX_INIT_FAST);
	(void) my_hash_init(&tapioca_open_tables, system_charset_info, 32, 0, 0,
			(my_hash_get_key) tapioca_get_key, 0, 0);

	tapioca_hton->state = SHOW_OPTION_YES;
	tapioca_hton->create = tapioca_create_handler;
	tapioca_hton->flags = HTON_CAN_RECREATE;
	tapioca_hton->commit = ha_tapioca_commit;
	tapioca_hton->rollback = ha_tapioca_rollback;

	// Entering administrative critical section for metadata and such
	pthread_mutex_lock(&tapioca_mutex);
	rv = tapioca_parse_config();
	if (rv) DBUG_RETURN(rv);

	if (!th_global_enabled)
	{
		if (init_administrative_connection() < 0)
		{
			printf( "TAPIOCA: Couldn't get administrative cxn to tapioca!\n" );
			fflush(stdout);
			DBUG_RETURN(-1);
		}
	}

	uint16_t num_bptrees_read;	
	rv = reload_tapioca_bptree_metadata(&num_bptrees_read);
	pthread_mutex_unlock(&tapioca_mutex);

	if (rv == -1)
	{
		printf("TAPIOCA: Failed to reload Tapioca B+Tree data\n");
		DBUG_RETURN(-1);
	}
	if (num_bptrees_read > 0) 
	{
		printf("Read %d existing b+trees from storage layer\n",num_bptrees_read);
	}


	DBUG_RETURN(0);
}

static int tapioca_parse_config() {
	DBUG_ENTER("tapioca_parse_config");

	FILE *cfg_fd = my_fopen("./tapioca.cfg", O_RDONLY, MYF(MY_WME));

	if (cfg_fd == NULL)
	{
		printf("TAPIOCA: Couldn't open tapioca config file!\n");
		printf("TAPIOCA: Did you place tapioca.cfg in the mysql data dir?\n");
		DBUG_RETURN(-1);
	}

	printf("TAPIOCA: Parsing config file...\n");
	fflush(stdout);
	size_t line_sz = 100;
	const char delimiters[] = " ";
	char *token;
	char *c_line = (char *) malloc(sizeof(char) * line_sz);
	int node = 0;

	while(getline(&c_line, &line_sz, cfg_fd) != -1 &&
			node < TAPIOCA_MAX_NODES_PER_MYSQL)
	{

		token = strtok(c_line, delimiters);
		if(token == NULL) continue;
		strncpy(tapioca_nodes[node].address, token, 128);
		token = strtok(NULL, delimiters);
		if(token == NULL) continue;
		tapioca_nodes[node].port = atoi(token);
		token = strtok(NULL, delimiters);
		if(token == NULL) continue;
		tapioca_nodes[node].mysql_instance_num = atoi(token);
		printf("Node %d host %s : %d , MySQL Instance ID %d\n", node,
				tapioca_nodes[node].address, tapioca_nodes[node].port,
				tapioca_nodes[node].mysql_instance_num);
		node++;
	}
	num_tapioca_nodes = node;
	printf("%d nodes read\n", num_tapioca_nodes);
	fflush(stdout);

	fflush(stdout);

	if (num_tapioca_nodes < 0)
	{
		printf( "TAPIOCA: Must provide valid tapioca node information "
				"in tapioca.cfg\n" );
		fflush(stdout);
		DBUG_RETURN(-1);
	}
	my_fclose(cfg_fd, MYF(MY_WME));
	local_execution_id = 1;
	DBUG_RETURN(0);
}

static int reload_tapioca_bptree_metadata(uint16_t *num_bptrees_read)
{
	DBUG_ENTER("reload_tapioca_bptree_metadata");
	int rv;
	my_hash_clear(&tapioca_bptrees);
	(void) my_hash_init(&tapioca_bptrees, system_charset_info, 32, 0, 0,
			(my_hash_get_key) tapioca_bptree_info_get_key, 0, 0);
	const char* bpt_meta_key = TAPIOCA_BPTREE_META_KEY;
	// TODO We will fail badly if this buffer goes > 64k, max tapioca buf size
	char buf[TAPIOCA_MAX_VALUE_SIZE];
	memset(buf,0, TAPIOCA_MAX_VALUE_SIZE);
	rv = tapioca_get(th_global, (uchar *) bpt_meta_key,
			(int) strlen(bpt_meta_key), buf, TAPIOCA_MAX_VALUE_SIZE);

	if (rv <= 0)
	{
		*num_bptrees_read = 0;
		printf("No b+tree metadata found, assuming fresh system\n");
		DBUG_PRINT("ha_tapioca", ("No metadata found, assuming fresh system"));
		DBUG_RETURN(0);
	}
	
	// TODO Verify that rv is sending the proper buffer size
	struct tapioca_bptree_info *bpt_map = 
		unmarshall_bptree_info(buf,(size_t) rv, num_bptrees_read);
	DBUG_PRINT( "ha_tapioca",
		("unmarshalled %d bptrees, inserting to glob meta hash", 
		 *num_bptrees_read));
	
	for (int i = 0; i < *num_bptrees_read; i++)
	{
		if (bpt_map->is_active)
		{
			if (my_hash_insert(&tapioca_bptrees, (uchar*) bpt_map))
				DBUG_PRINT("ha_tapioca",
				("Could not insert into bptree info hash"));
			DBUG_PRINT("ha_tapioca", ("Inserting bpt %s into hash", 
						  bpt_map->full_index_name));
		}
		bpt_map++;
	}
	
	DBUG_PRINT( "ha_tapioca",
			("unmarshalled %ul bptrees and inserted to metahash",
					tapioca_bptrees.records));
	DBUG_RETURN(0);

}

static int tapioca_done_func(void *p)
{
	int error = 0;
	DBUG_ENTER("tapioca_done_func");

	if (tapioca_open_tables.records)
		error = 1;
	my_hash_free(&tapioca_open_tables);
	pthread_mutex_destroy(&tapioca_mutex);

	DBUG_RETURN(error);
}

static TAPIOCA_SHARE * get_share(const char *table_name, TABLE *table)
{
	TAPIOCA_SHARE *share;
	uint length;
	char *tmp_name;
	pthread_mutex_lock(&tapioca_mutex);
	length = (uint) strlen(table_name);

	if (!(share = (TAPIOCA_SHARE*) my_hash_search(&tapioca_open_tables,
			(uchar*) table_name, length)))
	{
		if (!(share = (TAPIOCA_SHARE *) my_multi_malloc(
				MYF(MY_WME | MY_ZEROFILL), &share, 
				sizeof(*share), &tmp_name,
				length + 1, NullS)))
		{
			pthread_mutex_unlock(&tapioca_mutex);
			return NULL;
		}

		share->use_count = 0;
		share->table_name_length = length;
		share->table_name = tmp_name;

		strcpy(share->table_name, table_name);
		if (my_hash_insert(&tapioca_open_tables, (uchar*) share))
			goto error;
		thr_lock_init(&share->lock);
		pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
	}
	share->use_count++;
	pthread_mutex_unlock(&tapioca_mutex);

	return share;

	error: pthread_mutex_destroy(&share->mutex);
#if MYSQL_VERSION_ID>=50500
	my_free(share);
#else
	my_free(share, MYF(0));
#endif

	return NULL;
}

static int free_share(TAPIOCA_SHARE *share)
{
	pthread_mutex_lock(&tapioca_mutex);
	if (!--share->use_count)
	{
		my_hash_delete(&tapioca_open_tables, (uchar*) share);
		thr_lock_delete(&share->lock);
		pthread_mutex_destroy(&share->mutex);
#if MYSQL_VERSION_ID>=50500
		my_free(share);
#else
	my_free(share, MYF(0));
#endif
	}
	pthread_mutex_unlock(&tapioca_mutex);

	return 0;
}

static handler*
tapioca_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
	return new (mem_root) ha_tapioca(hton, table);
}

ha_tapioca::ha_tapioca(handlerton *hton, TABLE_SHARE *table_arg) :
	handler(hton, table_arg)
{
//	tapioca_write_opened = FALSE;
	thrloc = NULL;
	handler_tapioca_client_id = -1;
	pk_tbpt_id = -1;
	handler_opened = false;
}

static const char *ha_tapioca_exts[] =
{ NullS };

const char **
ha_tapioca::bas_ext() const
{
	return ha_tapioca_exts;
}

/*@
 * Returns a new tapioca connection selected in round-robin fashion from the
 * configured list of tapioca nodes in mysql-data-dir/tapioca.cfg
 * Returns the selected node in *node_id
 * \warning Requires synchronisation!
 */
tapioca_handle * ha_tapioca::init_tapioca_connection(int *node_id)
{
	DBUG_ENTER("ha_tapioca::init_tapioca_connection");
	// Default connection bpt_id is 0 for any direct tapioca access
	DBUG_PRINT("ha_tapioca", ("Attempting to open connection to tapioca..."));
	conn_counter++;
	*node_id = conn_counter % num_tapioca_nodes;
	tapioca_node_config nc = tapioca_nodes[*node_id];
	tapioca_handle *th = tapioca_open(nc.address, nc.port);
	printf("COUNT ME! Tapioca session created %d\n counter %d", *node_id,
			conn_counter);

	if (th == NULL)
	{
		DBUG_PRINT("ha_tapioca", ("Could not open tapioca connection handle"));
		DBUG_RETURN(NULL);
	}
	DBUG_PRINT("ha_tapioca", ("Opened connection to tapioca"));
	DBUG_RETURN(th);
}

int ha_tapioca::open(const char *name, int mode, uint test_if_locked)
{
	DBUG_ENTER("ha_tapioca::open");
	int rv;
	DBUG_PRINT( "ha_tapioca", ("Checking for bptree id for %s , "
			"currently %d bpts", name, tapioca_bptrees.records));
	// I don't understand why this is necessary, but we seem not to have access
	// to the exact same string as *name anywhere in the handler later on
	handler_opened = true;
	strcpy(full_table_name, name);

	// <WARNING WARNING DANGER DANGER>
	/* FIXME Brutal hack for now; hard code the tables that we will
	 * store the rows still in a separate tapioca key; default is store in-node
	 */
	is_row_in_node = true;
	const char * sep_row_tables[] = 
		{ "customer", "stock", "warehouse", "district"};
	for (int i = 0; i < 4; i++)
	{
		if (strcmp(sep_row_tables[i], table->s->table_name.str) == 0)
		{
			is_row_in_node = false;
			break;
		}
	}
	
	is_row_in_node = true;
	
	// </WARNING WARNING DANGER DANGER>

	if (!(share = get_share(name, table)))
		DBUG_RETURN(1);

	pthread_mutex_lock(&tapioca_mutex);

	if(handler_tapioca_client_id > -1) {
		// This handler was already assigned to a tapioca connection!
		printf("Here be dragons. tapioca client id was %d !\n",
				handler_tapioca_client_id);
		assert(0);
	}
	if(thrloc == NULL) create_or_set_thrloc(table->in_use);
	if (thrloc == NULL)
	{
		printf("Attempt to retrieve thrloc in open failed in THD %p\n",
				table->in_use);
		goto open_exception;
	}
	handler_tapioca_client_id = thrloc->tapioca_client_id;

	rv = create_or_set_tsession(table->in_use);
	if (!rv)
	{
		printf("Attempt to retrieve tsession failed in THD %p, thrloc %p\n",
				table->in_use, thrloc);
		goto open_exception;
	}


	thrloc->open_tables++;

	printf("Opening tapioca table, thd %p thrloc * %p bpt* %p\n",
			table->in_use, thrloc, thrloc->th);
	fflush(stdout);
	if (thrloc->th == NULL || thrloc == NULL) goto open_exception;
	rv = tapioca_get(thrloc->th, (uchar *) name, (int32_t) strlen(name),
			&tapioca_table_id, sizeof(int32_t));
	DBUG_PRINT( "ha_tapioca",
			("Fetched tapioca_table_id %d for %s rv %d tabname len %d",
			tapioca_table_id, name, rv, strlen(name)));

	if (rv <= 0) goto open_exception;

	pthread_mutex_unlock(&tapioca_mutex);

	memset(tmp_row_buf,0,TAPIOCA_MAX_VALUE_SIZE);
	thr_lock_data_init(&share->lock, &lock, NULL);

	DBUG_RETURN(0);

	open_exception: pthread_mutex_unlock(&tapioca_mutex);

	printf("TAPIOCA: Failure when opening table %s\n",
			table->s->table_name.str);
	fflush(stdout);
	DBUG_RETURN(-1);

}

// TODO Audit this method as it appears to cause problems in highly concurrent 
// executions
int ha_tapioca::close(void)
{
	int rc = 0;
	assert(is_thrloc_sane(thrloc));
	DBUG_ENTER("ha_tapioca::close");

	pthread_mutex_lock(&tapioca_mutex);
	// By the time we get here, table->in_use will have been nulled out
	THD *curthd = _current_thd();

	//printf("Current open count %d, curthd %p \n", thrloc->open_tables,curthd);
	fflush(stdout);

	thrloc->open_tables--;
	handler_tapioca_client_id = -1;
	if (thrloc->open_tables <= 0)
	{
		if(thrloc->th_enabled)
		{
			printf("Closing bpt *%p thr id %d\n", thrloc->th,curthd->thread_id);
			fflush(stdout);
			thrloc->th_enabled = 0;
			tapioca_close(thrloc->th);
		}
	}
	thd_set_ha_data(curthd, tapioca_hton, thrloc);

	pthread_mutex_unlock(&tapioca_mutex);

	DBUG_RETURN(free_share(share));
}

/*@ For the current auto-inc hack, it was assumed to be enough to check if a
 * field was NULL and also an autoinc, but, in some cases the field appears to
 * be a perfectly good 'zero', so for now we'll assume this is a case that 
 * needs an autoinc generated 
 */
inline int ha_tapioca::is_autoinc_needed(Field *field, const uchar *buf) 
{
	if (is_field_null(field, buf)) return 1;
	// FIXME This prevents a zero-valued key being entered into a autoinc PK!
	if (field->val_int() == 0) return 1;
	return 0;	
	
}

inline int ha_tapioca::is_field_null(Field *field, const uchar *buf) 
{
	if (!field->null_ptr) 
	{
		return 0;
	}
	
	int off = field->null_ptr - table->record[0];
	
	if (buf[off] & field->null_bit)
	{
		return 1;
	}
	return 0;	
	
}

inline uchar * ha_tapioca::get_next_mem_slot() {
	// Alas, the pre-alloc'd heap idea was well-intentioned but also a fail
	// FIXME Use slow and safe approach of zero-filled max value size
	return 
	(uchar *) my_malloc(TAPIOCA_MAX_VALUE_SIZE, MYF(MY_WME | MY_ZEROFILL));
}

/*@ Re-construct the key buffer we'll send to tapioca, including header */
// FIXME This method badly needs to be improved to handle VARCHAR and non-latin1
// character fields!
// The documentation for ha_innobase::store_key_val_for_row has valuable 
// information on how to parse the key buffer format
uchar *ha_tapioca::construct_tapioca_key_buffer(const uchar *key, uint key_len, 
						uint idx, size_t *buf_sz, 
						bool incl_header)
{
	DBUG_ENTER("ha_tapioca::construct_tapioca_key_buffer");
	uchar *k, *kptr, *kptr_new;
	kptr = k = get_next_mem_slot();
	if (incl_header)
	{
		kptr = write_tapioca_buffer_header(k);
	}
	KEY *key_info = &table->key_info[idx];
	KEY_PART_INFO *key_part = key_info->key_part;
	KEY_PART_INFO *key_end = key_part + key_info->key_parts;

	uint key_buf_offset = 0;
	//uint out_buf_offset = 0;
	for (; key_part != key_end; ++key_part)
	{
		Field *field = key_part->field;
		if (field->null_ptr != NULL) {
			// Field is nullable -- skip the null byte in the key buf
			key_buf_offset += 1;
		}
		if (field->type() == MYSQL_TYPE_VARCHAR)
		{
			// Take the varchar / char format as-is
			memcpy(kptr, key+key_buf_offset, key_part->store_length);
			kptr += key_part->store_length;
			key_buf_offset += key_part->store_length;
		} 
		else if (field->type() == MYSQL_TYPE_STRING)
		{
			// Take the varchar / char format as-is
			memcpy(kptr, key+key_buf_offset, key_part->field->max_data_length());
			kptr += key_part->field->max_data_length();
			key_buf_offset += key_part->field->max_data_length();
			
		}
		else
		{
			kptr_new = field->pack(kptr, key + key_buf_offset);
			key_buf_offset += (kptr_new - kptr);
			kptr = kptr_new;
		}
		if (key_buf_offset >= key_len) break;
	}

	*buf_sz = (int) (kptr - k);
	assert(*buf_sz < TAPIOCA_MAX_VALUE_SIZE);
	DBUG_PRINT("ha_tapioca", ("Generated pk buffer size %d", *buf_sz));
	DBUG_RETURN(k);
}

/*@ Ugly hack in order to handle varchars in mysql */
void ha_tapioca::handle_varchar(KEY_PART_INFO *key_part, uchar **k, const uchar *buf) 
{
	uchar *kptr = *k;
	Field *field = key_part->field;
	if (((Field_varstring*)field)->length_bytes == 1)
	{
		kptr[0] = *buf;
		kptr[1] = 0;
		memcpy(kptr+2, buf + 1, key_part->length);
	} 
	else 
	{
		memcpy(kptr, buf, key_part->store_length);
	}
	assert((key_part->length + 2) == key_part->store_length);
	kptr += key_part->length + 2;
	*k = kptr;
}

// FIXME Until we can properly support index variable length keys on the storage end, 
// we have to force things into buffers that are the maximum size
uchar * ha_tapioca::construct_idx_buffer_from_row(const uchar *buf, size_t *buf_sz,
						  int idx, bool incl_header)
{
	KEY *key_info = &table->key_info[idx];
	KEY_PART_INFO *key_part = key_info->key_part;
	KEY_PART_INFO *key_end = key_part + key_info->key_parts;
	uchar *k, *kptr, *kptr_new;
	uint key_buf_offset = 0;
	kptr = k = get_next_mem_slot();
	if (incl_header)
	{
		kptr = write_tapioca_buffer_header(k);
	}

	for (; key_part != key_end; ++key_part)
	{
		//memset(kptr, 0, key_part->store_length);
		// TODO Implement proper null handling
		Field *field = key_part->field;
		// FIXME Quick hack to "support" autoinc; 
		if (table->next_number_field == field &&
			is_autoinc_needed(field, buf)) {
			kptr_new = field->pack(kptr, (const uchar *)&current_auto_inc);
		}
		// sigh. hack this the way NDB does it
		else if (field->type() == MYSQL_TYPE_VARCHAR)
		{
			handle_varchar(key_part, &kptr, (buf + key_part->offset));
		}
		else if (field->type() == MYSQL_TYPE_STRING)
		{
			field->pack(kptr, buf + key_part->offset);
			kptr += key_part->field->max_data_length();
		}
		else 
		{
			kptr = field->pack(kptr, buf + key_part->offset);
		}
	}
	*buf_sz = (int)((kptr - k));
	assert(*buf_sz < TAPIOCA_MAX_VALUE_SIZE);
	DBUG_PRINT("ha_tapioca", ("Generated pk buffer size %d", *buf_sz));
	return(k);
}

/*@ Returns a buffer with the packed contents of mysql row in *buf; 
 * writes the length of packed data incl. header, to *buf_size
 * Row format:
 * -------------------------------------------
 * | Row Length <int32> | MySQL row data ... |
 * -------------------------------------------
 * Row Length is packed with the length of the packed buffer to follow
 */
uchar * ha_tapioca::construct_tapioca_row_buffer(const uchar *buf, size_t * buf_sz)
{
	uchar *v= get_next_mem_slot();
	uchar *vptr = v;

	// we'll skip past the spot for size as we need to compute it below
	vptr += sizeof(int32_t);
	//vptr_prev = vptr;
	memcpy(vptr, buf, table->s->null_bytes); // copy any null bytes the row may contain
	vptr += table->s->null_bytes;

	my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
	for(Field **field = table->field; *field; field++){
		if(!((*field)->is_null())){
			// FIXME Quick hack to "support" autoinc; 
			if (table->next_number_field == (*field) &&
				is_autoinc_needed((*field), buf)) 
			{
				vptr = (*field)->pack(vptr, (const uchar *)&current_auto_inc);
			}
			else 
			{
				vptr = (*field)->pack(vptr, buf + ((*field)->ptr - buf));
			}
		}
	}

	*buf_sz = (size_t)(vptr - v);
	assert(*buf_sz < TAPIOCA_MAX_VALUE_SIZE);
	dbug_tmp_restore_column_map(table->read_set, org_bitmap);

	// prepend packed row size to front of buffer (not incl. header!)
	int32_t sz = *buf_sz - (sizeof(int32_t));
	memcpy(v, &sz, sizeof(int32_t));
	return v;
}

int ha_tapioca::delete_from_index(const uchar *buf, int idx)
{
	uchar *key, *val;
	uchar nb = '\0';
	size_t k_sz, v_sz;
	
	tapioca_bptree_id tbpt_id = get_tbpt_id_for_idx(idx);
	if (tbpt_id == -1) return -1;
	
	if (idx == table->s->primary_key)
	{
		key = construct_idx_buffer_from_row(buf, &k_sz,
						    table->s->primary_key, false);
		if (is_row_in_node)
		{
			val = construct_tapioca_row_buffer(buf, &v_sz);
		}
		else 
		{
			val = &nb;
			v_sz = 1;
			tapioca_put(thrloc->th, key, k_sz, val, v_sz);
		}
	}
	else 
	{
		key = construct_idx_buffer_from_row(buf, &k_sz, idx, false);
		val = construct_idx_buffer_from_row(buf, &v_sz, table->s->primary_key, false);
	}
	
	int rv = tapioca_bptree_delete(thrloc->th, tbpt_id, key, k_sz, val, v_sz);
	my_free(key, MYF(MY_WME));	
	my_free(val, MYF(MY_WME));	
	return rv;
}

int ha_tapioca::insert_to_index(const uchar *buf, int idx, uchar *row, size_t row_sz)
{		
	int rv;
	size_t pk_sz, sk_sz;
	uchar *pk;
	uuid_t u;
	
	tapioca_bptree_id tbpt_id = get_tbpt_id_for_idx(idx);
	if (tbpt_id == -1) return -1;
	
	if(table_has_pk()) 
	{
		pk = construct_idx_buffer_from_row(buf, &pk_sz,
						   table->s->primary_key, false);
	}
	else 
	{
		uuid_generate(u);
		pk = (uchar *) &u;
		pk_sz = sizeof(uuid_t);
	}
	
	sk_sz = pk_sz;
	uchar *sk = pk;
	if (idx != table->s->primary_key)
	{
		sk = construct_idx_buffer_from_row(buf, &sk_sz, idx, false);
	}
	
	if (idx == table->s->primary_key)
	{
		if(is_row_in_node)
		{
			rv = tapioca_bptree_insert(thrloc->th, tbpt_id, pk, pk_sz,
					row, row_sz);
		}
		else
		{
			char nb = '\0';
			rv = tapioca_bptree_insert(thrloc->th, tbpt_id, pk, pk_sz, 
					(void *) &nb, 1);
		}
	}
	else
	{
		// This is a secondary key; the 'value' is the primary key buffer itself
		rv = tapioca_bptree_insert(thrloc->th, tbpt_id, sk, sk_sz,
								   pk, pk_sz);
		my_free(sk, MYF(MY_WME));	
	}
	my_free(pk, MYF(MY_WME));	
	return(rv);
}

inline bool ha_tapioca::table_has_pk() {
	// open_binary_frm() in table.cc:1545
	// share->primary_key = MAX_KEY; // we do not have a primary key
	return (table->s->primary_key != MAX_KEY);
	
}

/*@
 *  Write out index data for this row buffer;
 *  return a buffer in pk_buf to be used for the PK */
int ha_tapioca::write_all_indexes(uchar *buf, uchar *row, size_t row_sz)
{
	DBUG_ENTER("ha_tapioca::write_all_indexes");
	assert(is_thrloc_sane(thrloc));
	int fn_pos = 0, rv;

	for (int i = 0; i < table->s->keys; i++)
	{
		rv = insert_to_index(buf, i, row, row_sz);

		if (rv == BPTREE_OP_RETRY_NEEDED) {
			// Don't return an error here -- just flag the tx for rollback whenever
			// it tries to commit
			//DBUG_RETURN(HA_ERR_TOO_MANY_CONCURRENT_TRXS);
			thrloc->tx_cant_commit = true;
		}
		else if (rv < 0) {
			goto index_exception;
		}
		else if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		}
	}
	
	// FIXME Fix the copy/paste once we confirm this is working
	if (!table_has_pk()) 
	{
		rv = insert_to_index(buf, MAX_KEY, row, row_sz);

		if (rv == BPTREE_OP_RETRY_NEEDED) {
			// Don't return an error here -- just flag the tx for rollback whenever
			// it tries to commit
			//DBUG_RETURN(HA_ERR_TOO_MANY_CONCURRENT_TRXS);
			thrloc->tx_cant_commit = true;
		}
		else if (rv < 0) {
			goto index_exception;
		}
		else if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		}
		
	}

	DBUG_RETURN(0);

index_exception:
	printf("TAPIOCA: Error in write_all_indexes at pos %d, rv %d\n",fn_pos, rv);
	fflush(stdout);
	DBUG_RETURN(-1);
}

int ha_tapioca::write_row(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::write_row");
	assert(is_thrloc_sane(thrloc));
	int fn_pos = 0, rv;
	size_t row_sz = 0, pk_sz = 0;

	if (thd_sql_command(table->in_use) == SQLCOM_LOAD && write_rows % 20 == 0)
		tapioca_commit(thrloc->th);

	fn_pos = 1;
	if (table->next_number_field) {
		current_auto_inc = rand();
	}

	uchar *row = construct_tapioca_row_buffer(buf, &row_sz);
	// This row will be relatively long lived, make a copy from slot
	rv = write_all_indexes(buf, row, row_sz);

	if(rv > 0) DBUG_RETURN(rv);
	if (rv < 0) goto write_exception;

	fn_pos = 2;

	// If we don't write the row into the btree, put it into a separate k/v
	if (!is_row_in_node)
	{
		uchar *pk = construct_idx_buffer_from_row(buf, &pk_sz, 
												  table->s->primary_key, true);
		if (tapioca_put(thrloc->th, pk, pk_sz, row, row_sz) == -1) 
			goto write_exception;
		my_free(pk, MYF(MY_WME));
	}

	//my_free(row, MYF(MY_WME));
	fn_pos = 3;
	write_rows++;

	statistic_increment(table->in_use->status_var.ha_write_count, &LOCK_status);
	statistic_increment(stats.records, &LOCK_status);

	DBUG_RETURN(0);

	write_exception:
	printf("TAPIOCA: Exception in write_row pos %d\n", fn_pos);
	DBUG_RETURN(-1);
}

tapioca_bptree_id ha_tapioca::get_tbpt_id_for_idx(int idx) 
{
	tapioca_table_session *tsession;
	
	if (idx >= table->s->keys && idx != MAX_KEY) return -1;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		return -1;
	}

	return tsession->tbpt_ids[idx];
}

int ha_tapioca::update_indexes(const uchar *old_data, const uchar *new_data)
{
	int rv;
	MY_BITMAP idx_map; 
	my_bitmap_map idx_map_buf[bitmap_buffer_size(MAX_FIELDS)];
	// Check if any indexed columns were updated in any index
	for (int i =0; i < table->s->keys; i++)
	{
		bitmap_init(&idx_map, idx_map_buf, table->s->fields, FALSE);
		table->mark_columns_used_by_index_no_reset(i, &idx_map);
		bitmap_intersect(&idx_map, table->write_set);
			
		if (!bitmap_is_clear_all(&idx_map))
		{
			// One of the columns of this index has been written to
			
			rv = delete_from_index(old_data,i);
			if (rv != BPTREE_OP_SUCCESS) return rv;
			
			size_t new_row_sz;
			uchar *new_row = construct_tapioca_row_buffer(new_data, &new_row_sz);
			
			rv = insert_to_index(new_data, i, new_row, new_row_sz);
			if (rv != BPTREE_OP_SUCCESS) return rv;
			
		}
	}
	return BPTREE_OP_SUCCESS;
	
}

int convert_to_mysql_error(int bptree_rv) 
{
	// TODO Make this more complete
	switch (bptree_rv) {
		case BPTREE_OP_KEY_NOT_FOUND:
			return HA_ERR_KEY_NOT_FOUND;
		case BPTREE_OP_EOF:
			return HA_ERR_END_OF_FILE;
		case BPTREE_ERR_DUPLICATE_KEY_INSERTED:
			return HA_ERR_FOUND_DUPP_KEY;
		case BPTREE_OP_SUCCESS:
			return 0;
		default:
			return -1;
	}
	
}

int ha_tapioca::update_row(const uchar *old_data, uchar *new_data)
{
	DBUG_ENTER("ha_tapioca::update_row");
	
	int rv, dummy = 0;
	assert(is_thrloc_sane(thrloc));

	if(!table_has_pk()) 
	{
		printf("MoSQL currently does not support deleting or updating"
				" rows with no primary key!\n");
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	
	rv = update_indexes(old_data, new_data);
	if (rv != BPTREE_OP_SUCCESS)  DBUG_RETURN(convert_to_mysql_error(rv));
	
	tapioca_bptree_id tbpt_id = get_tbpt_id_for_idx(table->s->primary_key);
	if (tbpt_id == -1) return -1;
	
	size_t pk_sz, new_row_sz;
	uchar *pk;
	uchar *new_row = construct_tapioca_row_buffer(new_data, &new_row_sz);
	if (is_row_in_node)
	{
		pk = construct_idx_buffer_from_row(new_data, &pk_sz, 
											table->s->primary_key, false);
		rv = tapioca_bptree_update(thrloc->th, tbpt_id, pk, pk_sz, 
								   new_row, new_row_sz);
	}
	else
	{
		pk = construct_idx_buffer_from_row(new_data, &pk_sz, 
											table->s->primary_key, true);
		rv = tapioca_put(thrloc->th, pk, pk_sz, new_row, new_row_sz);
	}

	my_free(pk, MYF(MY_WME));
	my_free(new_row, MYF(MY_WME));
	DBUG_RETURN(convert_to_mysql_error(rv));

}

int ha_tapioca::delete_row(const uchar *buf)
{
	DBUG_ENTER("ha_tapioca::delete_row");
	assert(is_thrloc_sane(thrloc));
	if(!table_has_pk()) 
	{
		printf("MoSQL currently does not support deleting or updating"
				" rows with no primary key!\n");
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	
	
	int rv = delete_from_index(buf, table->s->primary_key);
	rv = convert_to_mysql_error(rv);
	
	DBUG_RETURN(rv);
}

uchar *ha_tapioca::write_tapioca_buffer_header(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::write_tapioca_buffer_header");
	uchar *bptr = buf;
	DBUG_PRINT( "ha_tapioca",
			("Prepending tapioca_table_id %d to buffer header",
				tapioca_table_id));

	const unsigned char hdr = TAPIOCA_ROW_PACKET_HEADER;
	memcpy(bptr, &hdr, 1);
	bptr++;
	memcpy(bptr, &tapioca_table_id, sizeof(int32_t));
	bptr += sizeof(int32_t);
	DBUG_RETURN(bptr);
}

inline int get_tapioca_header_size()
{
	return sizeof(int32_t) + 1;
}

// Utility method to determine at what level of the index a key buffer resides
inline int ha_tapioca::get_key_level(const uchar *key, int len)
{
	int pos = 1, size = 0;
	KEY pk_info = table->key_info[table->s->primary_key];
	KEY_PART_INFO *key_part = pk_info.key_part;
	KEY_PART_INFO *key_end = key_part + pk_info.key_parts;
	for (; key_part != key_end; ++key_part)
	{
		size += key_part->field->max_data_length();
		if (size >= len) return pos;
		pos++;
	}
	return pos;
}

inline int ha_tapioca::get_pk_length()
{
	int size = 0;
	if (!table_has_pk( )) return sizeof(uuid_t);
	KEY pk_info = table->key_info[table->s->primary_key];
	KEY_PART_INFO *key_part = pk_info.key_part;
	KEY_PART_INFO *key_end = key_part + pk_info.key_parts;
	for (; key_part != key_end; ++key_part)
	{
		if (key_part->field->type() == MYSQL_TYPE_VARCHAR) {
			size += key_part->store_length;
		} else {
			size += key_part->field->max_data_length();
		}
	}
	return size;
}
/**	Unpack mysql row stored in *v into *buf
 @param buf - the mysql buffer that we populate the row into
 @param v - the packed buffer we got from tapioca
 */
int ha_tapioca::unpack_row_into_buffer(uchar *buf, uchar *v)
{
	DBUG_ENTER("ha_tapioca::unpack_row_into_buffer");
	int32_t stored_buf_size, buf_size = 0;
	uchar *bptr = buf;
	uchar *vptr = v;
	memcpy(&stored_buf_size, vptr, sizeof(int32_t));
	vptr += sizeof(int32_t);

	my_bitmap_map *org_bitmap =
			dbug_tmp_use_all_columns(table, table->read_set);

	// Copy any null bytes
	memcpy(bptr, vptr, table->s->null_bytes);
	vptr += table->s->null_bytes;
	const uchar *vptrc = vptr;
	const uchar *vptrc_prev = vptr;
	for (Field **field = table->field; *field; field++)
	{
		if (!((*field)->is_null()))
		{
		// TODO Verify exactly when this method changed
		#if MYSQL_VERSION_ID<50500
			vptrc = (*field)->unpack(bptr + (*field)->offset(table->record[0]),
					vptrc);
		#else
			// FIXME Verify proper way to call this in later versions
			vptrc = (*field)->unpack(bptr + (*field)->offset(table->record[0]),
					vptrc,0,0);

		#endif
		}

		buf_size += (int) (vptrc - vptrc_prev);
		vptrc_prev = vptrc;
	}

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN(0);
}

int count_bits_set(uint v) 
{
	//uint v; // count the number of bits set in v
	uint c; // c accumulates the total bits set in v
	for (c = 0; v; c++)
	{
		v &= v - 1; // clear the least significant bit set
	}
	return c;
}
inline bool ha_tapioca::is_index_buffer_exact_match(uint index, key_part_map keypart_map) 
{
	KEY *key_info = &table->s->key_info[index];
	return (key_info->key_parts == count_bits_set(keypart_map));
}

int ha_tapioca::index_read(uchar * buf, const uchar * key, uint key_len,
		enum ha_rkey_function find_flag)
{
	DBUG_ENTER("ha_tapioca::index_read");
	uchar *v, *k, *kptr;
	int pk_size, rv;
	int32_t ksize, vsize;
	size_t key_buf_sz;
	tapioca_bptree_id tbpt_id;
	tapioca_handle *th = thrloc->th;
	
	bool use_primary = (active_index == table->s->primary_key || 
				active_index == MAX_KEY );
	if (use_primary)
	{
		tbpt_id = get_tbpt_id_for_idx(table->s->primary_key);
	}
	else 
	{
		tbpt_id = get_tbpt_id_for_idx(active_index);
	}

	v = get_next_mem_slot();

	uint actual_index = (active_index == MAX_KEY) ? table->s->primary_key :
				active_index;
	k = construct_tapioca_key_buffer(key, key_len, actual_index, 
					&key_buf_sz, true);
	kptr = k + get_tapioca_header_size();
	
	rv = tapioca_bptree_search(th, tbpt_id, kptr, 
				   key_buf_sz - get_tapioca_header_size(), v, 
				   &vsize);
	
	if(use_primary && key_len == get_pk_length())
	{
		if (rv == BPTREE_OP_KEY_NOT_FOUND) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		
		if (is_row_in_node)
		{
			rv = unpack_row_into_buffer(buf, v);
		}
		else 
		{
			rv = get_row_by_key(buf, k);
		}
		DBUG_RETURN(rv);
	}
	
	my_free(k, MYF(MY_WME));	
	my_free(v, MYF(MY_WME));	
	
	if (rv < 0)
	{
		printf("TAPIOCA: Exception in exact bpt_search in index_read\n");
		DBUG_RETURN(-1);
	}
	
	// Not an exact match and we've set the cursor on the storage layer
	DBUG_RETURN(index_fetch(buf, false));
}

/*@
 * Return the row for the given primary key
 * Expects *k to contain a valid key buffer 
 * *buf should point to the buffer from the calling mysql function where the
 * unpacked row will go
 * If we store the row in the PK index, this will do a search; otherwise it
 * will be a direct get to the storage layer
 */
int ha_tapioca::get_row_by_key(uchar *buf, uchar *k) // , int ksize)
{
	DBUG_ENTER("ha_tapioca::read_index_key");
	int rv, vsize;
	int ksize = get_pk_length() + get_tapioca_header_size();
	uchar *rowbuf = get_next_mem_slot();
	if (is_row_in_node)
	{
		// We want to skip the tapioca row header stuff
		assert(pk_tbpt_id != -1);
		rv = tapioca_bptree_search(thrloc->th, pk_tbpt_id,
					k + get_tapioca_header_size(),
					ksize - get_tapioca_header_size(), 
					rowbuf, &vsize);
		if (rv == BPTREE_OP_KEY_NOT_FOUND) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		if (rv < 0) DBUG_RETURN(-1);
	}
	else
	{
		rv = tapioca_get(thrloc->th, k, ksize, rowbuf, TAPIOCA_MAX_VALUE_SIZE);
		if (rv == -1) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	rv = unpack_row_into_buffer(buf, rowbuf);
	my_free(rowbuf, MYF(MY_WME));	
	DBUG_RETURN(rv);
}

int ha_tapioca::index_fetch(uchar *buf, bool first)
{
	DBUG_ENTER("ha_tapioca::index_fetch");
	assert(is_thrloc_sane(thrloc));
	int ksize, vsize, pk_size, rv;
	uchar *k, *v, *kptr, *vptr, *pk_ptr; 
	tapioca_bptree_id tbpt_id;
	k = get_next_mem_slot();
	v = get_next_mem_slot();
	kptr = write_tapioca_buffer_header(k);
	vptr = write_tapioca_buffer_header(v);
	ref_length = get_pk_length();
	// It seems that active_index 64 means "no" index; so we want the primary
	bool use_primary = (active_index == table->s->primary_key ||
					active_index == MAX_KEY );
	if (use_primary)
	{
		tbpt_id = get_tbpt_id_for_idx(table->s->primary_key);
	}
	else 
	{
		tbpt_id = get_tbpt_id_for_idx(active_index);
	}
	tapioca_handle *th = thrloc->th;

	if (first)
	{
		rv = tapioca_bptree_index_first(th, tbpt_id, 
						kptr, &ksize, vptr, &vsize);
	}
	else
	{
		rv = tapioca_bptree_index_next(th, tbpt_id, 
					       kptr, &ksize, vptr, &vsize);
	}

	// If this is the primary key; the key buffer is the key itself;
	// otherwise it's pointed to in the value
	if (use_primary)
	{
		pk_ptr = k;
		pk_size = ksize;
	}
	else
	{
		pk_ptr = v;
		pk_size = vsize;
	}

	if (rv == BPTREE_OP_KEY_FOUND)
	{
		if (is_row_in_node && use_primary)
		{
			rv = unpack_row_into_buffer(buf, vptr);
		}
		else
		{
			rv = get_row_by_key(buf, pk_ptr);
		}

		my_free(k, MYF(MY_WME));	
		my_free(v, MYF(MY_WME));	
		DBUG_RETURN(rv);
	}
	else if (rv == BPTREE_OP_EOF)
	{
		my_free(k, MYF(MY_WME));	
		my_free(v, MYF(MY_WME));	
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}
	else
	{
		printf("TAPIOCA: Exception in bpt_next/first in idx_ftch"
				"rv %d \n",rv);
		DBUG_RETURN(-1);
	}
}
/* See tests/bptree_cursor.c for test cases outlining expected behaviour of
 * cursor functions  */
int ha_tapioca::index_next(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::index_next");

#ifndef BPTREE_BUFFERING
	DBUG_RETURN(index_fetch(buf, false));
#else
	DBUG_RETURN(index_fetch_buffered(buf, false));
#endif
}
int ha_tapioca::index_first(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::index_first");
#ifndef BPTREE_BUFFERING
	DBUG_RETURN(index_fetch(buf, true));
#else
	DBUG_RETURN(index_fetch_buffered(buf, true));
#endif

}

int ha_tapioca::index_prev(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::index_prev");
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
}
int ha_tapioca::index_last(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::index_last");
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
}

int ha_tapioca::rnd_init(bool scan)
{
	DBUG_ENTER("ha_tapioca::rnd_init");
	rows_written = -1;
	ref_length = get_pk_length(); // + get_tapioca_header_size();
	//stats.records = 0;
	
	// Ensure we are on the PK-index
	//bps->bpt_id = index_to_bpt_id[0];
	DBUG_RETURN(0);
	//	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_tapioca::rnd_end()
{
	DBUG_ENTER("ha_tapioca::rnd_end");
	rows_written = -1;
	DBUG_RETURN(0);
}

int ha_tapioca::rnd_next(uchar *buf)
{
	// Remember that we need to do n+1 calls to rnd_next if we have n rows
	// thanks to this nice little bug:
	// http://bugs.mysql.com/bug.php?id=47124
	int rv;
	DBUG_ENTER("ha_tapioca::rnd_next");
	if (rows_written < 0)
	{
//		rv = index_first(buf);
#ifdef BPTREE_BUFFERING
		rv = index_fetch_buffered(buf,true);
#else
		rv = index_fetch(buf,true);
#endif
		rows_written = 1;
		DBUG_RETURN(rv);
	}
	else
	{
		rows_written++;
#ifdef BPTREE_BUFFERING
		DBUG_RETURN(index_fetch_buffered(buf,false));
#else
		DBUG_RETURN(index_fetch(buf,false));
#endif
	}
}

/**
 * For the mysql row, store the primary key value in this->ref
 @see
 filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
 */

void ha_tapioca::position(const uchar *record)
{
	DBUG_ENTER("ha_tapioca::position");
	size_t pk_len;
	ref_length = get_pk_length(); 
	uchar *pk_ptr = construct_idx_buffer_from_row((uchar *) record, &pk_len, 
						   table->s->primary_key, false);
	assert(pk_len == ref_length);
	memcpy(ref, pk_ptr, ref_length);
	my_free(pk_ptr, MYF(MY_WME));
	
	DBUG_VOID_RETURN;
}


/**
 @brief
	Return the row for whatever key happened to be stored in *pos
 @see
 filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
 */
int ha_tapioca::rnd_pos(uchar *buf, uchar *pos)
{
	DBUG_ENTER("ha_tapioca::rnd_pos");
	int rv;
	tapioca_bptree_id tbpt_id;
	
	rv = index_read(buf, pos, ref_length, HA_READ_KEY_EXACT);

	DBUG_RETURN(rv);
}


int ha_tapioca::info(uint flag)
{
	// TODO Review InnoDB code to start returning much more data back to opt.
	DBUG_ENTER("ha_tapioca::info");
	// This oddly seems to be all that InnoDB is doing 
	if (flag & HA_STATUS_ERRKEY) {
		errkey = 0;
	}
	DBUG_RETURN(0);
}

/**
 @brief
 extra() is called whenever the server wishes to send a hint to
 the storage engine. The myisam engine implements the most hints.
 ha_innodb.cc has the most exhaustive list of these hints.

 @see
 ha_innodb.cc
 */
int ha_tapioca::extra(enum ha_extra_function operation)
{
	DBUG_ENTER("ha_tapioca::extra");
	DBUG_RETURN(0);
}

// TODO It really shoudlnt' be so difficult to implement this...
int ha_tapioca::delete_all_rows()
{
	DBUG_ENTER("ha_tapioca::delete_all_rows");
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
}

/*@
 * Initialize new bptree(s) for this table
 * \warning Requires synchronisation!
 */
tapioca_table_session * ha_tapioca::initialize_new_bptree_thread_data()
{
	DBUG_ENTER("ha_tapioca::initialize_new_bptree_thread_data");
	struct tapioca_bptree_info *bpt_info;
	assert(is_thrloc_sane(thrloc));
	tapioca_table_session *tsession = (tapioca_table_session *) my_malloc(
			sizeof(tapioca_table_session), MYF(MY_WME | MY_ZEROFILL));

	// Quick and dirty way to support "index" 64
	tsession->tbpt_ids = (tapioca_bptree_id *) my_malloc(
			sizeof(tapioca_bptree_id) * (MAX_KEY+1), MYF(MY_WME | MY_ZEROFILL));
	assert (tsession != NULL);
	assert (tsession->tbpt_ids != NULL);

	tapioca_bptree_id *tbptr = tsession->tbpt_ids;

	KEY* key_info = table->key_info;
	for (int i = 0; i < table->s->keys; i++)
	{
		struct tapioca_bptree_info *bpt_info;
		String full_index_str(TAPIOCA_MAX_TABLE_NAME_LEN * 2);
		get_table_index_key(&full_index_str, full_table_name, key_info->name);
		const char *full_index_name = full_index_str.ptr();

		if (!(bpt_info = (struct tapioca_bptree_info *) my_hash_search(&tapioca_bptrees,
				(uchar*) full_index_name, strlen(full_index_name))))
		{
			// Try to reload -- if we fail throw an error
			uint16_t num_bptrees_read;
			int rv = reload_tapioca_bptree_metadata(&num_bptrees_read);
			if (num_bptrees_read <= 0) {
				printf("TAPIOCA: Couldn't find bpt meta info for %s\n",
					full_index_name);
			} else {
				printf("Re-read %d trees from storage, try again\n",
					   num_bptrees_read);
			}
			
			DBUG_RETURN(NULL);
		}
		if (bpt_info->bpt_id == -1)
		{
			printf("Could not find bpt_id for %s\n", full_table_name);
			DBUG_RETURN(NULL);
		}

		uint32_t new_bptree_exec_id = get_next_bptree_execution_id(thrloc->node_id);

		// Here we do not actually write-back anything , so the b+tree must have already
		// been initialized, and we are simply starting a new session
		printf("CONNECT to table %s idx %d bpt %d with exec id %d\n",
			table->s->table_name.str, i, bpt_info->bpt_id, new_bptree_exec_id);
		
		bptree_insert_flags insert_flags = BPTREE_INSERT_ALLOW_DUPES;
		if (key_info->flags & HA_NOSAME)
		{
				insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		}
		*tbptr = tapioca_bptree_initialize_bpt_session_no_commit(thrloc->th,
				bpt_info->bpt_id, BPTREE_OPEN_ONLY, insert_flags, 
				new_bptree_exec_id);

		if (*tbptr < 0) DBUG_RETURN(NULL);

		if(i == table->s->primary_key) pk_tbpt_id = *tbptr;

		tapioca_bptree_set_num_fields(thrloc->th, *tbptr, key_info->key_parts);
		KEY_PART_INFO *key_part = key_info->key_part;
		for (int j = 0; j < key_info->key_parts; j++)
		{
			switch (key_part->field->type())
			{
			case MYSQL_TYPE_TINY:
				printf("Setting bpt %d index part %d to int8 \n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_INT_8);
				break;
			case MYSQL_TYPE_SHORT:
				printf("Setting bpt %d index part %d to int16\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_INT_16);
				break;
			case MYSQL_TYPE_INT24: // ie. MEDIUMINT
				printf(
					"INT24 not supported! Setting bpt %d idx pt %d: int16cmp\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_INT_16);
				break;
			case MYSQL_TYPE_LONG:
				printf("Setting bpt %d index part %d to int32\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_INT_32);
				break;
			case MYSQL_TYPE_LONGLONG:
				printf("Setting bpt %d index part %d to int64\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_INT_64);
				break;
			case MYSQL_TYPE_VARCHAR:
				// TODO Find out proper way to do collation; use memcmp for now
				printf("Setting bpt %d index part %d to varchar memcmp\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j,
						key_part->store_length,
						BPTREE_FIELD_COMP_MYSQL_VAR_STRNCMP);
				break;
			case MYSQL_TYPE_STRING:
				// TODO Find out proper way to do collation; use memcmp for now
				printf("Setting bpt %d index part %d to char memcmp\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j,
						key_part->field->max_data_length(),
						BPTREE_FIELD_COMP_MYSQL_STRNCMP);
				break;
			default:
				printf("Setting bpt %d index part %d to default memcmp\n",
						*tbptr, j);
				tapioca_bptree_set_field_info(thrloc->th, *tbptr, j, key_part->store_length,
						BPTREE_FIELD_COMP_MEMCMP);
				break;

			}
			key_part++;
		}
		// Will full_table_name be set by now?
		printf("Setting idx %d to bps->bpt_id %d, loc iter %d, exec_id %d \n",i,
				*tbptr, local_execution_id, new_bptree_exec_id);
		fflush(stdout);
		DBUG_PRINT("ha_tapioca",
				("Setting idx %d to bps->bpt_id %d", i, *tbptr));
		tbptr++;
		key_info++;
	}
	
	if (!table_has_pk()) 
	{
		String full_index_str(TAPIOCA_MAX_TABLE_NAME_LEN * 2);
		get_table_index_key(&full_index_str, full_table_name, 
							MOSQL_IMPLICIT_PK_NAME);
		
		bpt_info = (struct tapioca_bptree_info *) my_hash_search(&tapioca_bptrees,
				(uchar*) full_index_str.ptr(), strlen(full_index_str.ptr()) );
		if(bpt_info == NULL || bpt_info->bpt_id == -1 ) DBUG_RETURN(NULL);
		
		uint32_t new_bptree_exec_id = get_next_bptree_execution_id(thrloc->node_id);
		tapioca_bptree_id t =
			tapioca_bptree_initialize_bpt_session_no_commit(thrloc->th,
				bpt_info->bpt_id, BPTREE_OPEN_ONLY, BPTREE_INSERT_UNIQUE_KEY, 
				new_bptree_exec_id);
		if (t < 0) DBUG_RETURN(NULL);
		tsession->tbpt_ids[table->s->primary_key] = pk_tbpt_id = t;
		
		tapioca_bptree_set_num_fields(thrloc->th, t, 1);
		printf("Setting bpt %d index part %d to memcmp\n", t, 1);
		tapioca_bptree_set_field_info(thrloc->th, t, 0, sizeof(uuid_t),
				BPTREE_FIELD_COMP_MEMCMP);
		
	}
	
	strcpy(tsession->full_table_name, full_table_name);

	if (my_hash_insert(&(thrloc->tsessions), (uchar*) tsession))
	{
		DBUG_RETURN(NULL);
	}

	DBUG_RETURN(tsession);
}
void * ha_tapioca::initialize_thread_local_space()
{
	DBUG_ENTER("ha_tapioca::initialize_thread_local_space");
	tapioca_thrloc *_thrloc = (tapioca_thrloc *)
		my_malloc(sizeof(tapioca_thrloc), MYF(MY_ZEROFILL));
	thrloc = _thrloc;
	thrloc->accesses = 1;
//	tapioca_handle *th = init_tapioca_connection(&thrloc->node_id);
//	thrloc->th_enabled = 1;
	thrloc->th_enabled = 0;
	thrloc->tx_active = false;
	thrloc->tx_cant_commit = false;
	thrloc->num_tables_in_use = 0;
	thrloc->open_tables = 0;
	//thrloc->val_buf_pos = -1;
	//thrloc->val_buf_sz = -1;
	thrloc->should_not_be_written = 0xDEADBEEF;
	thrloc->tapioca_client_id = -1;
	(void) my_hash_init(&(thrloc->tsessions), system_charset_info, 32, 0, 0,
			(my_hash_get_key) tsession_get_key, 0, 0);

	DBUG_RETURN(thrloc);

}

/*@ Create or grab the thread-local storage for this connection
 * \warning Requires synchronisation!
 */
int ha_tapioca::create_or_set_thrloc(THD *thd)
{
	DBUG_ENTER("ha_tapioca::create_or_set_thrloc");
//	tapioca_thrloc *thrloc;
	thrloc = (tapioca_thrloc *) thd_get_ha_data(thd, tapioca_hton);
	if (thrloc == NULL)
	{
		printf("Create thrloc in thread id: %d\n", thd->thread_id);
		initialize_thread_local_space();
		if (thrloc == NULL)
			DBUG_RETURN(0);
		thd_set_ha_data(thd, tapioca_hton, thrloc);
	}
	else
	{
		thrloc->accesses++;
		// Our thread is still alive, but we previously closed all
		// open table references
	}
	if (!thrloc->th_enabled)
	{
		thrloc->th = init_tapioca_connection(&thrloc->node_id);
		if (thrloc->th == NULL)
		{
			DBUG_RETURN(0);
		}
		thrloc->th_enabled = 1;
		thrloc->tapioca_client_id = tapioca_client_id(thrloc->th);
	}
	DBUG_RETURN(1);
}

/* Gets or creates the bptree session for this table
 * Assumes that thrloc space has been initialized!
 * \warning Requires synchronisation!
 */
int ha_tapioca::create_or_set_tsession(THD *thd)
{
	DBUG_ENTER("ha_tapioca::create_or_set_tsession");
	assert(is_thrloc_sane(thrloc));
	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		if (!initialize_new_bptree_thread_data()) DBUG_RETURN(0);
		thd_set_ha_data(thd, tapioca_hton, thrloc);
	}
	else {
		// Cache the bpt_id of the primary key since the session already exists
		pk_tbpt_id = tsession->tbpt_ids[table->s->primary_key];
	}
	assert (pk_tbpt_id >= 0 && pk_tbpt_id < 10000);
	DBUG_RETURN(1);
}

int ha_tapioca::external_lock(THD *thd, int lock_type)
{
	DBUG_ENTER("ha_tapioca::external_lock");
	DBUG_PRINT("ha_tapioca",
			("entered external_lock with lock_type %d", lock_type));
	int rv;
	int fn_pos = 0;

	if (thd_sql_command(thd) == SQLCOM_ALTER_TABLE || thd_sql_command(thd)
			== SQLCOM_CREATE_INDEX)
	{
		DBUG_RETURN( HA_ERR_WRONG_COMMAND);
	}
	// I.e. when we enter
	if (lock_type != F_UNLCK)
	{
		pthread_mutex_lock(&tapioca_mutex);

		create_or_set_thrloc(thd);
		if (thrloc == NULL)
		{
			printf("TAPIOCA: Attempt to retrieve thrloc in extlock "
					"failed in THD %p\n", thd);
			goto external_lock_exception;
		}
		fn_pos = 1;
		DBUG_PRINT("ha_tapioca", ("ext_lock type %d, thrloc accesses %d thd_id %lu"
				"#tabs %d \n", lock_type, thrloc->accesses, thd->real_id,
				thrloc->num_tables_in_use));

		rv = create_or_set_tsession(thd);
		if (!rv)
		{
			printf("TAPIOCA: Attempt to retrieve tsession failed in extl THD %p, "
					"thrloc %p\n", thd, thrloc);
			goto external_lock_exception;
		}
		fn_pos = 2;

		thrloc->num_tables_in_use++;
		write_rows = 0;
		bool all_tx;
		all_tx = thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN
				| OPTION_TABLE_LOCK);

		if (all_tx)
		{
			trans_register_ha(thd, true, tapioca_hton);
			//thrloc->tx_active = true;
		}
		else
		{
			trans_register_ha(thd, false, tapioca_hton);
			//thrloc->tx_active = false;
		}
		thrloc->tx_active = true;

		pthread_mutex_unlock(&tapioca_mutex);
	}
	else
	{
		pthread_mutex_lock(&tapioca_mutex);
		create_or_set_thrloc(thd);
		if (thrloc == NULL)
		{
			printf("TAPIOCA: Attempt to retrieve thrloc in extlock "
					"failed in THD %p\n", thd);
			goto external_lock_exception;
		}
		// Is this the correct way to handle auto-commits?
		thrloc->num_tables_in_use--;
		// We should commit only when exiting ext_lock!
/*		if (thrloc->num_tables_in_use == 0)
		{
			if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)
					&& !thrloc->tx_active)
			{
				rv = tapioca_commit(thrloc->th);
				DBUG_PRINT("ha_tapioca",
						("Commit within external_lock() thdid %lu bpt* %p rv %d \n",
								thd->real_id, thrloc->th, rv));
				fflush(stdout);
				thrloc->tx_active = false;

				if (rv < 0) {
					rv = 12312;
					// We should only commit read-only transactions here; proper write transactions
					 //that could fail should be registered and handled by ha_tapioca_commit 
					 
					//DBUG_RETURN(HA_ERR_LOCK_TABLE_FULL);
					
					//DBUG_RETURN(0);
				}
				statistic_add(table->in_use->status_var.ha_savepoint_count, rv, &LOCK_status);
				statistic_increment( table->in_use->status_var.ha_savepoint_rollback_count, &LOCK_status);
			}

		}
		*/
		pthread_mutex_unlock(&tapioca_mutex);
	}

	DBUG_RETURN(0);

	external_lock_exception: pthread_mutex_unlock(&tapioca_mutex);
	printf("TAPIOCA: Exception in external_lock pos %d\n", fn_pos);
	DBUG_RETURN(-1);

}
int ha_tapioca::start_stmt(THD *thd, thr_lock_type lock_type)
{
	DBUG_ENTER("ha_tapioca::start_stmt");
	//	DBUG_PRINT("ha_tapioca", ("entered start_stmt with lock_type %d",
	// lock_type));
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
	/* Reference implementation from docs:
	 int error= 0;
	 my_txn *txn= (my_txn *) thd->ha_data[my_handler_hton.slot];

	 if (txn == NULL)
	 {
	 thd->ha_data[my_handler_hton.slot]= txn= new my_txn;
	 }
	 if (txn->stmt == NULL && !(error= txn->tx_begin()))
	 {
	 txn->stmt= txn->new_savepoint();
	 trans_register_ha(thd, FALSE, &my_handler_hton);
	 }
	 return error;
	 */
}

THR_LOCK_DATA **
ha_tapioca::store_lock(THD *thd, THR_LOCK_DATA **to,
		enum thr_lock_type lock_type)
{
	// Lifted this code from ha_archive which seems to be a relatively
	// close match to our needs
	if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
	{
		/*
		 Here is where we get into the guts of a row level lock.
		 If TL_UNLOCK is set
		 If we are not doing a LOCK TABLE or DISCARD/IMPORT
		 TABLESPACE, then allow multiple writers
		 */

		if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)
				&& !thd_in_lock_tables(thd) && !thd_tablespace_op(thd))
			lock_type = TL_WRITE_ALLOW_WRITE;

		/*
		 In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
		 MySQL would use the lock TL_READ_NO_INSERT on t2, and that
		 would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
		 to t2. Convert the lock to a normal read lock to allow
		 concurrent inserts to t2.
		 */

		if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
			lock_type = TL_READ;

		lock.type = lock_type;
	}

	*to++ = &lock;

	return to;
}

int ha_tapioca::rename_table(const char * from, const char * to)
{
	DBUG_ENTER("ha_tapioca::rename_table ");
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
}

/**
 @brief
 Given a starting key and an ending key, estimate the number of rows that
 will exist between the two keys.

 @details
 end_key may be empty, in which case determine if start_key matches any rows.

 Called from opt_range.cc by check_quick_keys().

 @see
 check_quick_keys() in opt_range.cc
 TODO We need to do a better job of estimating this, because it is likely
 the cause for TPC-C queries that do not use more than the second level of an
 index
 FIXME If we have a statement like UPDATE .. WHERE k > 10; max_key will be
 NULL and this method will segfault!
*/
ha_rows ha_tapioca::records_in_range(uint inx, key_range *min_key,
		key_range *max_key)
{
	DBUG_ENTER("ha_tapioca::records_in_range");

	int key_lvl = get_key_level(min_key->key,min_key->length);
	int key_parts = table->key_info[inx].key_parts;
	int same = memcmp(min_key->key, max_key->key,
			(int)fmin(min_key->length, max_key->length));
	if(same == 0) key_lvl --;

	/* A crude function to estimate exponentially fewer rows as the number of
	 parts of the key increases */
	int x = key_lvl;
	int n = records();
	if (n == 0) n = 5000; // if n is not set, or 0, assume we have something
	int k = key_parts;
	int res = n * ( (exp10(-x) - exp10(-k)) / (exp10(-1)) ) + 1;

	DBUG_RETURN(res);
}

/*@ We will repurpose the REPAIR statement as a way of refreshing the
 * metadata for the tapioca tables that exist in a target tapiocadb
 * This is mostly important in cases where we have multiple mysql nodes
 * sharing the same db, and we want to sync up the tables
 */
int ha_tapioca::repair(THD* thd, HA_CHECK_OPT* check_opt)
{
	DBUG_ENTER("ha_tapioca::repair");
	int rv;
	uint16_t num_bptrees_read;
	pthread_mutex_lock(&tapioca_mutex);
	rv = reload_tapioca_bptree_metadata(&num_bptrees_read);
	pthread_mutex_unlock(&tapioca_mutex);
	DBUG_RETURN(rv);
}

int ha_tapioca::analyze(THD* thd, HA_CHECK_OPT* check_opt)
{
	DBUG_ENTER("ha_tapioca::analyze");
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*@ Repurpose the CHECK TABLE statement right now to simply dump the
 * bptree contents to the log file or run basic checks 
 */
int ha_tapioca::check(THD* thd, HA_CHECK_OPT* check_opt)
{
	DBUG_ENTER("ha_tapioca::check");
	assert(is_thrloc_sane(thrloc));
	int rv, final_rv = 0;
	tapioca_bptree_id *tbptr;
	tapioca_handle *th;
	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
		DBUG_RETURN(-1);

	tbptr = tsession->tbpt_ids;
	th = thrloc->th;

	for (int i = 0; i < table->s->keys; i++)
	{
		KEY *key_info = table->key_info;
		KEY_PART_INFO *key_part = key_info->key_part;
		KEY_PART_INFO *key_end = key_part + key_info->key_parts;


		// We've run out of CHECK Options so make QUICK FAST mean something
		// different!
		if ((check_opt->flags & T_QUICK) && (check_opt->flags & T_FAST))
		{
			printf("Seq. dump of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_DUMP_RECURSIVELY);
		}
		else if ((check_opt->flags & T_MEDIUM) && (check_opt->flags & T_FAST))
		{
			printf("Graphviz dump of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_DUMP_GRAPHVIZ);
		}
		else if (check_opt->flags & T_QUICK)
		{
			printf("Seq. dump of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_DUMP_SEQUENTIALLY);
		}
		else if (check_opt->flags & T_FAST)
		{
			printf("Recurs. scan of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_INDEX_RECURSIVE_SCAN);
		}
		else if (check_opt->flags & T_MEDIUM)
		{
			printf("Seq. verify of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_VERIFY_SEQUENTIALLY);
		}
		else if (check_opt->flags & T_EXTEND)
		{
			printf("Recurs. verif of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_VERIFY_RECURSIVELY);
		}
		printf(" rv %d\n", rv);
		if (rv <= 0) DBUG_RETURN(-1);
		tbptr++;
	}
	fflush(stdout);
	DBUG_RETURN(0);
}


/**
 @brief
 create() is called to create a table within mysql;
 All metadata for tables and such is found inside tapioca itself.
 key TAPIOCA_TABLE_AUTOINC will contain the integral value of the
 last created table. We'll also store a key in tapioca
 with the tablename containing the table id

 Called from handle.cc by ha_create_table().

 @see
 ha_create_table() in handle.cc
 */
int ha_tapioca::create(const char *name, TABLE *table_arg,
		HA_CREATE_INFO *create_info)
{
	DBUG_ENTER("ha_tapioca::create");
	int rv = 0;
	int32_t table_id = -1;
	int fn_pos = 0;

	if (!(share = get_share(name, table)))
	{
		printf("TAPIOCA: Exception in ::create to get_share\n");
		DBUG_RETURN(-1);
	}

	if (tapioca_nodes[0].mysql_instance_num != 0)
	{
		printf("TAPIOCA: This is not the first MySQL node; creating table"
				" locally but no global metadata will be changed.\n");
		uint16_t num_bptrees_read;
		DBUG_RETURN(reload_tapioca_bptree_metadata(&num_bptrees_read));
	}
	pthread_mutex_lock(&tapioca_mutex);

	if (!th_global_enabled) init_administrative_connection();

	/* In order to support multiple MySQL nodes, here we need to check if
	 the table already exists in the tapioca db */

	rv = tapioca_get(th_global, (uchar *) name, (int32_t) strlen(name),
			&table_id, sizeof(int32_t));

	if (rv > 0 && table_id >= 0)
	{ // Table exists; create local metadata
		printf("Existing table_id %d in tapioca found\n", table_id);
	}
	else if (rv == 0 || table_id <= -1)
	{ // This is a new/deleted table in the cluster
		fn_pos = 1;
		const char* str = TAPIOCA_TABLE_AUTOINC_KEY;

		table_id = 0;
		int tries = 0;
		rv = -1;
		
		tapioca_get(th_global, (char *)str, strlen(str), &table_id, sizeof(int32_t));
		table_id++;
		tapioca_put(th_global, (char *)str, strlen(str), &table_id, sizeof(int32_t));

		fn_pos = 2;

		printf("Got next table_id %d for %s tabname len %d, %d tries, rv %d\n",
				table_id, name, strlen(name), tries, rv);

		rv = tapioca_put(th_global, (uchar *) name,
				(int32_t) strlen(name), &table_id, sizeof(int32_t));
		if (rv == -1) goto create_exception;

		tapioca_table_id = table_id;

		////////////////////////////
		// Create new bpts
		printf("Creating table; # keys %d , flds %d, pk %d \n",
				table_arg->s->keys, table_arg->s->fields,
				table_arg->s->primary_key);
		fflush(stdout);
		KEY *ki = table_arg->key_info;
		for (int i = 0; i < table_arg->s->keys; i++)
		{
			fn_pos = 3;
			printf(" key idx %d name %s key flags %ld \n", i, ki->name,
					ki->flags);
			rv = create_new_bpt_id(name, ki->name, table_arg, i);
			if (rv == -1) goto create_exception;

			ki++;
		}
		if(table_arg->s->primary_key == MAX_KEY) 
		{
			printf(" implicit pk idx %d name %s key flags %ld \n", MAX_KEY, 
				   MOSQL_IMPLICIT_PK_NAME, 0);
			rv = create_new_bpt_id(name, MOSQL_IMPLICIT_PK_NAME, 
								   table_arg, MAX_KEY);
			if (rv == -1) goto create_exception;
			
		}
		fflush(stdout);
		fn_pos = 3;
		rv = tapioca_commit(th_global);
		if (rv < 0) goto create_exception;
	}
	else
	{
		goto create_exception;
	}
	pthread_mutex_unlock(&tapioca_mutex);
	DBUG_RETURN(0);

	create_exception:
	printf("TAPIOCA: Exception in ::create at pos %d\n", fn_pos);
	fflush(stdout);
	pthread_mutex_unlock(&tapioca_mutex);
	DBUG_RETURN(-1);

}

int ha_tapioca::create_new_bpt_id(const char *table_name,
		const char *index_name, TABLE *table_arg, int idx)
{
	DBUG_ENTER("ha_tapioca::create_new_bpt_id");
	int rv, mk = 0;
	int tries = 0, rv_p;
	const char* bpt_meta_key = TAPIOCA_BPTREE_META_KEY;
	void *buf;
	tapioca_bptree_id prev_bpt_id = -1, tbps_ignored;

	struct tapioca_bptree_info *bpt_info = (struct tapioca_bptree_info *) my_malloc(
			sizeof(struct tapioca_bptree_info), MYF(MY_ZEROFILL | MY_WME));

	bpt_info->bpt_id = -1;
	bpt_info->is_pk = (idx == table_arg->s->primary_key);
	bpt_info->is_active = 1;

	// Here we will get the bptid as part of a sequence maintain inside tapioca
	String full_index_str(TAPIOCA_MAX_TABLE_NAME_LEN * 2);
	get_table_index_key(&full_index_str, table_name, index_name);
	const char *full_index_name = full_index_str.ptr();

	const char* str = TAPIOCA_BPTREE_AUTOINC_KEY;
	rv = -1;
	
	tapioca_get(th_global,(char *)str, strlen(str), &prev_bpt_id, sizeof(tapioca_bptree_id));
	if (prev_bpt_id < 0) prev_bpt_id = 0;
	bpt_info->bpt_id = prev_bpt_id +1;
	tapioca_put(th_global,(char *)str, strlen(str), &bpt_info->bpt_id, sizeof(tapioca_bptree_id));
	strncpy(bpt_info->full_index_name, full_index_name, 128);

	mk = 1;
    bptree_insert_flags insert_flags = BPTREE_INSERT_ALLOW_DUPES;
	if (idx < MAX_KEY)
	{
		KEY *key_info = &table_arg->key_info[idx];
		if (key_info->flags & HA_NOSAME)
		{
				insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		}
	}
	else
	{
		// This is an implicit PK
		insert_flags = BPTREE_INSERT_UNIQUE_KEY;
	}


	// Make sure the metadata is created on tapioca for this table
	// Blow away whatever bpt_id might have existed there before;
	//	it will then be ready for when ::open is called
	tbps_ignored = tapioca_bptree_initialize_bpt_session(th_global,
			bpt_info->bpt_id, BPTREE_OPEN_OVERWRITE, insert_flags);
	mk = 2;
	if (tbps_ignored < 0)
	{
		rv = tbps_ignored;
		goto create_exception;
	}

	DBUG_PRINT("ha_tapioca", ("Creating new bpt_id %d for "
			"table/index %s\n", bpt_info->bpt_id, full_index_name));

	mk = 3;
	rv = my_hash_insert(&tapioca_bptrees, (uchar*) bpt_info);

	printf("Current bptree_info hash contents:\n");
	for (int i = 0; i < tapioca_bptrees.records; i++)
	{
		struct tapioca_bptree_info *t = (struct tapioca_bptree_info *) my_hash_element(
				&tapioca_bptrees, i);
		printf("bpt_id %d, is_pk %d, name %s namelen %d active %d\n",
				t->bpt_id, t->is_pk, t->full_index_name,
				strlen(t->full_index_name), t->is_active);
	}
	fflush(stdout);

	mk = 4;
	size_t bsize;
	buf = marshall_bptree_info(&bsize);
	rv = -1;
	tapioca_put(th_global, (uchar *) bpt_meta_key,
				(int) strlen(bpt_meta_key), buf, bsize);


	DBUG_RETURN(0);

	create_exception:
	printf("TAPIOCA: Error creating new bpt_id, pos %d, rv %d, tries %d \n",
			mk, rv, tries);
	fflush(stdout);
	DBUG_RETURN(-1);
}

uchar * ha_tapioca::marshall_bptree_info(size_t *bsize)
{
	DBUG_ENTER("ha_tapioca::marshall_bptree_info");
	int i;
    /* creates buffer and serializer instance. */
    msgpack_sbuffer *buffer = msgpack_sbuffer_new();
    msgpack_packer *pck = msgpack_packer_new(buffer, msgpack_sbuffer_write);
    msgpack_sbuffer_clear(buffer);
	struct tapioca_bptree_info* m;

	msgpack_pack_int16(pck, tapioca_bptrees.records);
	for (i = 0; i < tapioca_bptrees.records; i++)
	{
		m = (struct tapioca_bptree_info *) my_hash_element(&tapioca_bptrees, i);
		if (m == NULL) DBUG_RETURN(NULL);
		//memcpy(&m, bpt_info, sizeof(tapioca_bptree_info));
		msgpack_pack_int16(pck, m->bpt_id);
		msgpack_pack_raw(pck, sizeof(m->full_index_name));
		msgpack_pack_raw_body(pck, m->full_index_name, sizeof(m->full_index_name));
		msgpack_pack_int16(pck, m->is_active);
		msgpack_pack_int16(pck, m->is_pk);
	}

    msgpack_packer_free(pck);
    *bsize = buffer->size;
	uchar *p = (uchar *)buffer->data;
    free(buffer);
	DBUG_RETURN(p);
}


struct tapioca_bptree_info *
unmarshall_bptree_info(const char *buf, size_t sz, uint16_t *num_bptrees)
{
	DBUG_ENTER("unmarshall_bptree_info");
    msgpack_zone z;
    msgpack_zone_init(&z, 4096);
    msgpack_object obj;
    msgpack_unpack_return ret;
	size_t offset = 0;
	int i;
	
    ret = msgpack_unpack(buf, sz, &offset, &z, &obj);
    *num_bptrees =  (int16_t) obj.via.i64;
	
	struct tapioca_bptree_info *n = (struct tapioca_bptree_info *) my_malloc(
		sizeof(tapioca_bptree_info) * *num_bptrees, MYF(MY_WME | MY_ZEROFILL));
	struct tapioca_bptree_info *nptr = n;
	
	for (i = 0; i < *num_bptrees; i++)
	{
		ret = msgpack_unpack(buf, sz, &offset, &z, &obj);
		nptr->bpt_id = (tapioca_bptree_id) obj.via.i64;
		ret = msgpack_unpack(buf, sz, &offset, &z, &obj);
		strncpy(nptr->full_index_name, obj.via.raw.ptr, sizeof(nptr->full_index_name));
		ret = msgpack_unpack(buf, sz, &offset, &z, &obj);
		nptr->is_active = (int16_t) obj.via.i64;
		ret = msgpack_unpack(buf, sz, &offset, &z, &obj);
		nptr->is_pk = (int16_t) obj.via.i64;
		nptr++;
	}
	
	assert(ret == MSGPACK_UNPACK_SUCCESS);
	assert(*num_bptrees >= 0 && *num_bptrees < 16384);
	msgpack_zone_destroy(&z);
	DBUG_RETURN(n);
}

int ha_tapioca::delete_table(const char *name)
{
	DBUG_ENTER("ha_tapioca::delete_table");
	int bpt_to_rm, rv, i;
	int32_t table_id;
	//	KEY *ki = table->key_info;
	table_id = -1;

	tapioca_table_session *tsession;
	tapioca_thrloc *_thrloc;
	// By the time we get here, table->in_use will have been nulled out
	THD *curthd = _current_thd();

	// In case we drop and recreate the same table in the same active thread
	// make sure we have purged stale session data


	if (tapioca_nodes[0].mysql_instance_num != 0)
	{
		printf("TAPIOCA: This is not the first MySQL node; dropping table"
				" locally but no global metadata will be changed.\n");
		DBUG_RETURN(0);
	}
	printf("Dropping table %s\n", name);
	fflush(stdout);
	_thrloc = (tapioca_thrloc *) thd_get_ha_data(curthd, tapioca_hton);

	if (_thrloc != NULL)
	{
		// We can't assume that a session or thrloc exists,
		// because we could be dropping the schema on a first connection
		if (!(tsession = (tapioca_table_session *) my_hash_search(
				&_thrloc->tsessions, (uchar*) name, strlen(name))))
		{
			DBUG_PRINT("ha_tapioca",
					("Could not find tsession object for %s", name));
		}
		else
		{
			my_hash_delete(&_thrloc->tsessions, (uchar *) tsession);
		}
	}

	pthread_mutex_lock(&tapioca_mutex);

	// Place a table id of -1 to ensure tapioca thinks this table is deleted
	if (!th_global_enabled)
		init_administrative_connection();
	rv = tapioca_put(th_global, (uchar *) name, (int32_t) strlen(name),
			&table_id, sizeof(int32_t));

/*	 Here we must iterate through all the indexes of this table and deactivate
	 them and then resynchronize back to metadata store*/

/*	 Annoyingly, st_table struct is NULL by the time we get here, so we can't
	 depend on it to access the names of keys for this table :-/

	 FIXME As a workaround, iterate through the whole hash and search for prefix
	 matches; this will get very slow with many tables*/


	for (i = 0; i < tapioca_bptrees.records; i++)
	{
		struct tapioca_bptree_info *bpt_info =
				(struct tapioca_bptree_info *) my_hash_element(&tapioca_bptrees, i);
		if (bpt_info == NULL)
			continue; // this should not happen; upgrade to assert
		printf("\tChecking %s against %s\n", bpt_info->full_index_name, name);
		if (strncmp(bpt_info->full_index_name, name, strlen(name)) == 0)
		{
			printf("Found match to index %s , bpt_id %d, disabling\n",
					bpt_info->full_index_name, bpt_info->bpt_id);
			fflush(stdout);
		}
		bpt_info->is_active = 0;

	}


	// Re-synchronize bptree metadata back to tapioca
	size_t bsize;
	int attempts = 0;
	void *buf = marshall_bptree_info(&bsize);
	const char* bpt_meta_key = TAPIOCA_BPTREE_META_KEY;
	rv = -2;
	while(rv < 0 && attempts < 10)
	{
		rv = tapioca_put(th_global, (uchar *) bpt_meta_key,
				(int) strlen(bpt_meta_key), buf, bsize);

		rv = tapioca_commit(th_global);
		attempts++;
		if(rv < 0) usleep((rand() % 500) * 1000);
	}

	pthread_mutex_unlock(&tapioca_mutex);
	if (rv < 0)
	{
		printf( "TAPIOCA: Uh-oh. Failed to commit bptree metadata "
				"deleting %s after %d attempts, rv %d\n", name, attempts, rv);
		DBUG_RETURN(-1);
	}
	DBUG_RETURN(0);

}
void get_table_index_key(String *s, const char *table_name,
		const char *index_name)
{
	s->append(table_name);
	s->append('|');
	s->append(index_name);
}

//inline
bool is_thrloc_sane(tapioca_thrloc *thrloc)
{
	return (thrloc != NULL);
		//(thrloc->val_buf_pos <= TAPIOCA_MGET_BUFFER_SIZE) &&
		//(thrloc->val_buf_pos <= thrloc->val_buf_sz);
}

struct st_mysql_storage_engine tapioca_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var = 0;
static ulong srv_ulong_var = 0;

const char *enum_var_names[] =
{ "e1", "e2", NullS };

TYPELIB enum_var_typelib =
		{ array_elements(enum_var_names) - 1, "enum_var_typelib",
				enum_var_names, NULL };

static MYSQL_SYSVAR_ENUM(
		enum_var, // name
		srv_enum_var, // varname
		PLUGIN_VAR_RQCMDARG, // opt
		"Sample ENUM system variable.", // comment
		NULL, // check
		NULL, // update
		0, // def
		&enum_var_typelib); // typelib

static MYSQL_SYSVAR_ULONG(
		ulong_var,
		srv_ulong_var,
		PLUGIN_VAR_RQCMDARG,
		"0..1000",
		NULL,
		NULL,
		8,
		0,
		1000,
		0);

static struct st_mysql_sys_var* tapioca_system_variables[] =
{ MYSQL_SYSVAR(enum_var), MYSQL_SYSVAR(ulong_var), NULL };

mysql_declare_plugin(tapioca)
{ MYSQL_STORAGE_ENGINE_PLUGIN,
&tapioca_storage_engine,
"MoSQL",
"Alex Tomic, Daniele Sciascia, University of Lugano",
"MoSQL Storage Engine",
PLUGIN_LICENSE_GPL,
tapioca_init_func, /* Plugin Init */
tapioca_done_func, /* Plugin Deinit */
0x0001 /* 0.1 */,
NULL, /* status variables */
tapioca_system_variables, /* system variables */
NULL /* config options */
}
mysql_declare_plugin_end;
