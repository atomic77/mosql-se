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

static int reload_tapioca_bptree_metadata();
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
static uchar* tapioca_bptree_info_get_key(tapioca_bptree_info *bpt_info,
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
	if (thrloc->tx_active)
	{
		rv = tapioca_commit(thrloc->th);
		DBUG_PRINT("ha_tapioca",
				("Commit in ha_tapioca_commit tid %lu bpt* %p rv %d \n",
						thd->real_id, thrloc->th, rv));
		thrloc->tx_active = false;
		if (rv < 0)
			DBUG_RETURN( HA_ERR_TOO_MANY_CONCURRENT_TRXS);

		statistic_add(thd->status_var.ha_savepoint_count, rv, &LOCK_status);
		statistic_increment( thd->status_var.ha_savepoint_rollback_count, &LOCK_status);
	}
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
	if (thrloc->tx_active)
	{
		tapioca_rollback(thrloc->th);
		thrloc->tx_active = false;
	}
	DBUG_RETURN(0);
}

static int tapioca_init_func(void *p)
{
	DBUG_ENTER("tapioca_init_func");
	tapioca_hton = (handlerton *) p;
	int rv;

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
		if (init_administrative_connection() < 0)
		{
			printf( "TAPIOCA: Couldn't get administrative cxn to tapioca!\n" );
			fflush(stdout);
			DBUG_RETURN(-1);
		}

	rv = reload_tapioca_bptree_metadata();

	pthread_mutex_unlock(&tapioca_mutex);

	if (rv == -1)
	{
		printf("TAPIOCA: Failed to reload Tapioca B+Tree data\n");
		DBUG_RETURN(-1);
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

static int reload_tapioca_bptree_metadata()
{
	DBUG_ENTER("reload_tapioca_bptree_metadata");
	int rv;
	my_hash_clear(&tapioca_bptrees);
	(void) my_hash_init(&tapioca_bptrees, system_charset_info, 32, 0, 0,
			(my_hash_get_key) tapioca_bptree_info_get_key, 0, 0);
	const char* bpt_meta_key = TAPIOCA_BPTREE_META_KEY;
	// TODO We will fail badly if this buffer goes > 64k, max tapioca buf size
	uchar buf[TAPIOCA_MAX_VALUE_SIZE];
	bzero(buf, TAPIOCA_MAX_VALUE_SIZE);
	rv = tapioca_get(th_global, (uchar *) bpt_meta_key,
			(int) strlen(bpt_meta_key), buf, TAPIOCA_MAX_VALUE_SIZE);

	char *fmt = tpl_peek(TPL_MEM, buf, TAPIOCA_MAX_VALUE_SIZE);
	
	if (fmt == NULL)
	{
		DBUG_PRINT("ha_tapioca", ("No metadata found, assuming fresh system"));
		DBUG_RETURN(0);
	}
	
	assert(strncmp(fmt, bpt_meta_key, strnlen(bpt_meta_key, 100)) == 0);
	free(fmt);
	
	uint16_t num_bptrees;
	tapioca_bptree_info *bpt_map = unmarshall_bptree_info(buf, &num_bptrees);

	DBUG_PRINT( "ha_tapioca",
			("unmarshalled %d bptrees, inserting to glob meta hash", num_bptrees));
	
	for (int i = 0; i < num_bptrees; i++)
	{
		if (bpt_map->is_active)
		{
			if (my_hash_insert(&tapioca_bptrees, (uchar*) bpt_map))
				DBUG_PRINT("ha_tapioca",
						("Could not insert into bptree info hash"));
			DBUG_PRINT("ha_tapioca",
					("Inserting bpt %s into hash", bpt_map->full_index_name));
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
				MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share), &tmp_name,
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
	is_row_in_node = false;
	const char * sep_row_tables[] = { "item", "order_line", "orders", "new_order", "history"};
	for (int i = 0; i < 5; i++)
	{
		if (strcmp(sep_row_tables[i], table->s->table_name.str) == 0)
		{
			is_row_in_node = true;
			break;
		}
	}
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
			printf("Closing bpt *%p thr id %d\n", thrloc->th, curthd->thread_id);
			fflush(stdout);
			thrloc->th_enabled = 0;
			tapioca_close(thrloc->th);
		}
	}
	thd_set_ha_data(curthd, tapioca_hton, thrloc);

	pthread_mutex_unlock(&tapioca_mutex);

	DBUG_RETURN(free_share(share));
}

/* This method assumes that the bytes before *pk_buf have already been
 * initialized with the tapioca header; it will only write the actual pk data
 */
void ha_tapioca::construct_pk_buffer_from_row(uchar *pk_buf, uchar *buf, int *pk_len)
{
    // For the non-PK indexes, we will put in the value buffer the PK
    // so that the actual row can be looked up; this requires that we
    // collect the PK data first and then traverse through the remaining keys
    KEY *pk_info = &table->key_info[table->s->primary_key];
    KEY_PART_INFO *pk_part = pk_info->key_part;
    KEY_PART_INFO *pk_end = pk_part + pk_info->key_parts;
    uchar *pk_ptr = pk_buf;
    for (; pk_part != pk_end; ++pk_part)
	{
		DBUG_PRINT("ha_tapioca",
				("Index part on field %s", pk_part->field->field_name));
		if (pk_part->null_bit)
		{
			// If key value is null, move forward whatever the length is
			pk_ptr += pk_part->length;
			continue;
		}
		Field *field = pk_part->field;
		pk_ptr = field->pack(pk_ptr, buf + field->offset(buf));
	}
    *pk_len = (int)((pk_ptr - pk_buf));
    DBUG_PRINT("ha_tapioca", ("Generated pk buffer size %d", *pk_len));
}

/*@
 *  Write out index data for this row buffer;
 *  return a buffer in pk_buf to be used for the PK */
int ha_tapioca::write_index_data(uchar *buf, uchar *pk_buf, int *pk_len,
		uchar *pk_row, int pk_row_sz)
{
	DBUG_ENTER("ha_tapioca::write_index_data");
	assert(is_thrloc_sane(thrloc));
    int fn_pos = 0, rv;
    int num_keys = table->s->keys; // For now assume this is one
    int32_t dummy = 0;
	uchar v[TAPIOCA_MAX_VALUE_SIZE];
    tapioca_bptree_id *tbptr;
    tapioca_handle *th;

    construct_pk_buffer_from_row(pk_buf, buf, pk_len);

    KEY *key_info = table->key_info;
	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
		(uchar*) full_table_name, strlen(full_table_name)))) goto index_exception;
	fn_pos = 1;

	tbptr = tsession->tbpt_ids;
	th = thrloc->th;
	int32_t vsize;

	fn_pos = 2;

	for (int i = 0; i < table->s->keys; i++)
	{
		// We will write one tapioca value per index
		KEY_PART_INFO *key_part = key_info->key_part;
		KEY_PART_INFO *key_end = key_part + key_info->key_parts;
		bzero(v, TAPIOCA_MAX_VALUE_SIZE);
		uchar *vptr = v;
		bool is_null;
		int key_len = 0;

		for (; key_part != key_end; ++key_part)
		{
			is_null = false;
			if (key_part->null_bit)
			{
				// If key value is null, move forward whatever the length is
				is_null = true;
				vptr += key_part->field->max_data_length();
				continue;
			}
			Field *field = key_part->field;
			field->pack(vptr, buf + field->offset(buf));
			vptr += field->max_data_length();
		}

		int rv;
		bptree_insert_flags insert_flags = BPTREE_INSERT_ALLOW_DUPES;
		if (key_info->flags & HA_NOSAME)
			insert_flags = BPTREE_INSERT_UNIQUE_KEY;

		/* The PK index does not actually index anything, since we use tapioca
		 * to look up the actual row based on PK; non-PK indexes index PK values
		 * which then lookup rows with a straight tapioca_get call
		 */
		if (i == table->s->primary_key)
		{
			if(is_row_in_node)
			{
				fn_pos = 3;
				rv = tapioca_bptree_insert(th, *tbptr, v, (int) (vptr - v),
						pk_row, pk_row_sz, insert_flags);
			}
			else
			{
				fn_pos = 4;
				char nb = '\0';
				rv = tapioca_bptree_insert(th, *tbptr, v, (int) (vptr - v),
						(void *) &nb, 1, insert_flags);
			}
		}
		else
		{
			fn_pos = 5;
			rv = tapioca_bptree_insert(th, *tbptr, v, (int) (vptr - v), pk_buf,
					*pk_len, insert_flags);
		}
		
		if (rv == BPTREE_OP_RETRY_NEEDED) {
			usleep(100 * 1000);
			DBUG_RETURN(HA_ERR_TOO_MANY_CONCURRENT_TRXS);
		}
		else if (rv < 0) {
			goto index_exception;
		}
		else if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		}
		tbptr++;
		key_info++;
	}

	DBUG_RETURN(0);

index_exception:
	printf("TAPIOCA: Error in write_index_data at pos %d, rv %d\n",fn_pos, rv);
	fflush(stdout);
	DBUG_RETURN(-1);
}

/*@
 * Key: pack table id in the first 4 bytes and the pk itself in the remaining
 * Value: a header of 4 bytes containing the payload size, and the remainder of
 * the *buf structure
*/
int ha_tapioca::write_row(uchar *buf)
{
	DBUG_ENTER("ha_tapioca::write_row");
	assert(is_thrloc_sane(thrloc));
	int fn_pos = 0;
	int32_t packed_row_sz = 0, pk_sz = -1, rv;
	// buf that will contain the PK used in tapioca
	uchar pk[TAPIOCA_MAX_VALUE_SIZE];
	uchar v[TAPIOCA_MAX_VALUE_SIZE];
	bzero(pk, TAPIOCA_MAX_VALUE_SIZE);
	bzero(v, TAPIOCA_MAX_VALUE_SIZE);
	uchar *pk_ptr = pk;

	if (thd_sql_command(table->in_use) == SQLCOM_LOAD && write_rows % 20 == 0)
		tapioca_commit(thrloc->th);

	pk_ptr = write_tapioca_buffer_header(pk);

	fn_pos = 1;

    construct_tapioca_row_buffer(v, buf,&packed_row_sz);
    rv = write_index_data(buf, pk_ptr, &pk_sz, v, packed_row_sz);

	if(rv > 0) DBUG_RETURN(rv);
	if (rv < 0) goto write_exception;

	fn_pos = 2;

	// If we don't write the row into the btree, put it into a separate k/v
	if (!is_row_in_node)
	{
		if (tapioca_put(thrloc->th, pk, pk_sz + get_tapioca_header_size(),
				v, packed_row_sz) == -1) goto write_exception;
	}

	fn_pos = 3;
	write_rows++;

	statistic_increment(table->in_use->status_var.ha_write_count, &LOCK_status);
	statistic_increment(stats.records, &LOCK_status);

	DBUG_RETURN(0);

	write_exception:
	printf("TAPIOCA: Exception in write_row pos %d\n", fn_pos);
	DBUG_RETURN(-1);
}

/**
 * @brief
 * A method that transactionally retrieves the next auto-increment value
 * for a particular key ; assumes the contents of that key are integer!
 */
int get_next_autoinc(tapioca_handle *th, const char *key, int keylen,
		int *autoinc)
{
	DBUG_ENTER("ha_tapioca::get_next_autoinc");
	int a = -1;
	int rv = tapioca_get(th, (uchar *) key, keylen, &a, sizeof(int32_t));
	if (rv == -1) DBUG_RETURN(-1);
	DBUG_PRINT("ha_tapioca", ("next autoinc value retrieved %d ", a));
	*autoinc = (a <= 0) ? 1 : a + 1;
	DBUG_PRINT("ha_tapioca",
			("writing back new autoinc value of %d ", *autoinc));
	rv = tapioca_put(th, (uchar *) key, keylen, autoinc, sizeof(int32_t));
	if (rv == -1) DBUG_RETURN(-1);
	// This is a special case of a function that we will allow to commit
	DBUG_RETURN(tapioca_commit(th));
}

/*@ Expects a pre-allocated buffer v to place the packed contents of mysql row
 * in *buf; writes the length of packed data incl. header, to *buf_size
 * Row format:
 * -------------------------------------------
 * | Row Length <int32> | MySQL row data ... |
 * -------------------------------------------
 * Row Length is packed with the length of the packed buffer to follow
 */
void ha_tapioca::construct_tapioca_row_buffer(uchar *v, uchar *buf,
		int32_t * buf_size)
{
    uchar *vptr = v;
    uchar *vptr_prev = vptr;

	// we'll skip past the spot for size as we need to compute it below
	vptr += sizeof(int32_t);
	vptr_prev = vptr;
	memcpy(vptr, buf, table->s->null_bytes); // copy any null bytes the row may contain
	vptr += table->s->null_bytes;

    my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
    for(Field **field = table->field; *field; field++){
        if(!((*field)->is_null())){
            vptr = (*field)->pack(vptr, buf + (*field)->offset(buf));
        }
        *buf_size += (int)((vptr - vptr_prev));
        vptr_prev = vptr;
    }

    dbug_tmp_restore_column_map(table->read_set, org_bitmap);

	// prepend packed row size to front of buffer (not incl. header!)
	memcpy(v, buf_size, sizeof(int32_t));
	*buf_size += sizeof(int32_t);
}

int ha_tapioca::update_row(const uchar *old_data, uchar *new_data)
{
	DBUG_ENTER("ha_tapioca::update_row");
	int rv;
	assert(is_thrloc_sane(thrloc));
    int32_t buf_size = 0, pk_size = -1, old_pk_size, dummy = 0;
    uchar pk[TAPIOCA_MAX_VALUE_SIZE];
    uchar old_pk[TAPIOCA_MAX_VALUE_SIZE];
    uchar v[TAPIOCA_MAX_VALUE_SIZE];
    bzero(pk, TAPIOCA_MAX_VALUE_SIZE);
    bzero(v, TAPIOCA_MAX_VALUE_SIZE);
    uchar *pk_ptr = pk;
    uchar *old_pk_ptr = old_pk;
    tapioca_bptree_id *tbptr;
	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		DBUG_PRINT("ha_tapioca",
				("Could not find tsession object for %s", full_table_name));
		DBUG_RETURN(-1);
	}

	tbptr = tsession->tbpt_ids;
    pk_ptr = write_tapioca_buffer_header(pk);
    old_pk_ptr = write_tapioca_buffer_header(old_pk);

    construct_pk_buffer_from_row(pk_ptr, new_data, &pk_size);
    //construct_pk_buffer_from_row(old_pk_ptr, (uchar *)old_data, &old_pk_size);
    //old_pk_size += get_tapioca_header_size();


 /* FIXME We still need to properly support updating of actual indexed values*/
/*  if(old_pk_size != pk_size || !memcmp(pk_ptr, old_pk_ptr, pk_size))
    {
    	// The primary key for this table has changed; we must delete/insert
    	tapioca_bptree_delete(thrloc->th, tbptr[table->s->primary_key],
    			old_pk, old_pk_size, &dummy, sizeof(int32_t));
    	tapioca_bptree_insert(thrloc->th, tbptr[table->s->primary_key],	pk,
    			pk_size, &dummy, sizeof(int32_t), BPTREE_INSERT_UNIQUE_KEY);
    }
    else {
    	// We can call straight bptree_update
    	//tapioca_bptree_update(thrloc->th, tbptr[table->s->primary_key],
    	//		old_pk, pk_size, &dummy, sizeof(int32_t));
    }
*/
    construct_tapioca_row_buffer(v, new_data, &buf_size);
	if (is_row_in_node)
	{
		rv = tapioca_bptree_update(thrloc->th, tbptr[table->s->primary_key],
				pk_ptr, pk_size, v, buf_size);
		if (rv < 0) DBUG_RETURN (-1);
		if (rv == BPTREE_OP_KEY_NOT_FOUND) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}
	else
	{
		pk_size += get_tapioca_header_size();
		rv = tapioca_put(thrloc->th, pk, pk_size, v, buf_size);
		if(rv == -1) DBUG_RETURN (-1);
	}

	DBUG_RETURN(0);
}

int ha_tapioca::delete_row(const uchar *buf)
{
	DBUG_ENTER("ha_tapioca::delete_row");
	assert(is_thrloc_sane(thrloc));
    int32_t buf_size = 0, pk_size = -1, old_pk_size, dummy = 0;
    uchar pk[TAPIOCA_MAX_VALUE_SIZE];
    uchar v[TAPIOCA_MAX_VALUE_SIZE];
    bzero(pk, TAPIOCA_MAX_VALUE_SIZE);
    bzero(v, TAPIOCA_MAX_VALUE_SIZE);
    uchar *pk_ptr = pk;
    uchar *vptr = v;
    const uchar *bptr = buf;
    tapioca_bptree_id *tbptr;
	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		DBUG_PRINT("ha_tapioca",
				("Could not find tsession object for %s", full_table_name));
		DBUG_RETURN(-1);
	}

	tbptr = tsession->tbpt_ids;
    pk_ptr = write_tapioca_buffer_header(pk);

    construct_pk_buffer_from_row(pk_ptr, (uchar *)buf, &pk_size);

    tapioca_bptree_delete(thrloc->th, tbptr[table->s->primary_key],
    		pk_ptr, pk_size, &dummy, sizeof(int32_t));

	DBUG_RETURN(0);
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

inline int ha_tapioca::get_pk_length(bool incl_string)
{
	int size = 0;
	KEY pk_info = table->key_info[table->s->primary_key];
	KEY_PART_INFO *key_part = pk_info.key_part;
	KEY_PART_INFO *key_end = key_part + pk_info.key_parts;
	for (; key_part != key_end; ++key_part)
	{
		//size += key_part->field->max_data_length();
		size += incl_string ? key_part->field->max_data_length() :
			key_part->field->data_length();
	}
	return size;
}
/*@ Re-construct the key buffer we'll send to tapioca
*/
int ha_tapioca::construct_tapioca_key_buffer(uchar *k, const uchar *key,
		uint key_len, int *final_key_len)
{
	DBUG_ENTER("ha_tapioca::construct_tapioca_key_buffer");
	uchar *kptr = write_tapioca_buffer_header(k);
	KEY *key_info = &table->key_info[active_index];
	KEY_PART_INFO *key_part = key_info->key_part;
	KEY_PART_INFO *key_end = key_part + key_info->key_parts;

	/* The key buffer has a different format from regular rows, so we need
 	 * to keep track of these independently*/
	uint key_buf_offset = 0;
	//uint out_buf_offset = 0;
	for (; key_part != key_end; ++key_part)
	{
		Field *field = key_part->field;
		if (!field->is_null())
		{
			kptr = field->pack(kptr, key + key_buf_offset);
		}
		// if this is a partial key, stop here
		if (key_buf_offset + field->data_length() >= key_len) break; 
		// We want to traverse with the data_length, which does NOT include
		// the size of the 1/2 bytes that go along with the string! The output
		// buffer however will include those bytes
		key_buf_offset += field->data_length();
	}

	*final_key_len = (int) ((kptr - k));
	DBUG_RETURN(0);
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
#ifdef DEFENSIVE_MODE
			if ((*field)->type() == MYSQL_TYPE_VARCHAR ||
					(*field)->type() == MYSQL_TYPE_STRING) {
				// Assume we only have < 255 char strings for now
				int ln = (int)*(vptrc);
				// The following should always be true since max_data_len
				// includes the length byte
				assert ((*field)->max_data_length() > ln);
			}
#endif

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

int ha_tapioca::index_read_idx(uchar* buf, uint keynr, const uchar* key,
		uint key_len, enum ha_rkey_function find_flag)
{
	DBUG_ENTER("ha_tapioca::index_read_idx");
	DBUG_RETURN( HA_ERR_WRONG_COMMAND);
}

/*
 * Based on innodb code, seems that we only should need to handle index_read
 * find_flag == HA_READ_KEY_EXACT, HA_READ_PREFIX or 0; others are
 * probably rarely used
 *
 * READ_KEY_EXACT seems to be used in cases like PK(a,b) , WHERE a = 1;
 * and only a truncated key buffer is provided; this seems to suggest we
 * have to deduce whether this is a scan of exact PK match...
 */
int ha_tapioca::index_read(uchar * buf, const uchar * key, uint key_len,
		enum ha_rkey_function find_flag)
{
	DBUG_ENTER("ha_tapioca::index_read");
	assert(is_thrloc_sane(thrloc));

	// TODO Refactor this to make use of a thread-local buffer instead of
	// creating it on the stack every time
	uchar k[TAPIOCA_MAX_VALUE_SIZE];
	uchar v[TAPIOCA_MAX_VALUE_SIZE];
	bzero(k, TAPIOCA_MAX_VALUE_SIZE);
	bzero(v, TAPIOCA_MAX_VALUE_SIZE);
	uchar *pk_ptr = k;
	uchar *vptr;
	int pk_size, rv;
	int32_t final_key_len, ksize, vsize;
	int bps_id = (active_index == 64) ? 0 : active_index;

	tapioca_table_session *tsession;
	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		printf("TAPIOCA: Failed session lookup in index_read\n");
		DBUG_RETURN(-1);
	}

	tapioca_bptree_id tbpt_id = tsession->tbpt_ids[bps_id];
	tapioca_handle *th = thrloc->th;

	construct_tapioca_key_buffer((uchar*) (k), key, key_len, &final_key_len);
	vptr = write_tapioca_buffer_header(v);

	if (key_len < get_pk_length(false) || active_index != table->s->primary_key)
	{
		// partial key match -- make sure we search including the null bytes!
		uchar *kptr = k;

		kptr += get_tapioca_header_size();
		ksize = final_key_len - get_tapioca_header_size();
		rv = tapioca_bptree_search(th, tbpt_id, kptr, ksize, vptr, &vsize);

		// FIXME This logic needs to be cleaned up, or a special error code
		// for partial-key matches created so that we know to do a next()
		if (rv == BPTREE_OP_KEY_NOT_FOUND) 
		{
			rv = tapioca_bptree_index_next(th,tbpt_id,kptr,&ksize,vptr,&vsize);
			if (rv != BPTREE_OP_KEY_FOUND) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		}
		if (active_index == table->s->primary_key)
		{
			pk_ptr = k;
			pk_size = ksize;
		}
		else
		{
			/* This is a bit of a mess because the value buffer we'll pull
  				 in the case of non-PK index will be packed already, so we
				 don't need to use construct_tapioca_key_buffer as above */
			pk_ptr = v;
			pk_size = vsize + get_tapioca_header_size();
		}
	}
	else
	{
		// If this is an exact match, still want to ensure the cursor has been
		// set correctly as some functions expect to traverse the index
		uchar *kptr = k;

		kptr += get_tapioca_header_size();

		rv = tapioca_bptree_search(th, tbpt_id, kptr, 
			final_key_len - get_tapioca_header_size(), vptr, &vsize);
		// We could have been given a key that should return nothing
		// so do one scan first
		if (rv == BPTREE_OP_KEY_NOT_FOUND)
		{

			rv = tapioca_bptree_index_next(th, tbpt_id,kptr,&ksize,vptr,&vsize);
			if (rv != BPTREE_OP_KEY_FOUND) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		}
		if (rv < 0)
		{
			printf("TAPIOCA: Exception in exact bpt_search in index_read\n");
			DBUG_RETURN(-1);
		}
	}

	if (is_row_in_node && (active_index == table->s->primary_key))
	{
		rv = unpack_row_into_buffer(buf, vptr);
	}
	else {
		rv = read_index_key(buf, pk_ptr, v,
				get_tapioca_header_size() + get_pk_length(true));
	}

	table->status = 0;
	DBUG_RETURN(rv);
}

/* FIXME This method is still broken; we probably should move all the
 * buffering logic into tapioca
*/
int ha_tapioca::index_fetch_buffered(uchar *buf, bool first)
{

	DBUG_ENTER("ha_tapioca::index_fetch_buffered");
	assert(is_thrloc_sane(thrloc));
	int ksize, vsize,  rv = -1;
	int *pk_size;
	uchar rowbuf[TAPIOCA_MAX_VALUE_SIZE];
	uchar *pk_ptr;
	bool first_flag = first;
	bool has_rows = true;
	int bps_id = active_index;

	if (active_index == 64) bps_id = 0;

	tapioca_table_session *tsession;

	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		printf("TAPIOCA: session lookup failure in idx_ftch_buff\n");
		DBUG_RETURN(-1);
	}

	assert(is_thrloc_sane(thrloc));

	tapioca_bptree_id tbpt_id = tsession->tbpt_ids[bps_id];

	// Buffer is empty, or, we are starting a new scan; fill it up
	if(thrloc->val_buf_sz <= 0 || thrloc->val_buf_pos >= thrloc->val_buf_sz
			|| first)
	{
		rv = prefetch_tapioca_rows(tbpt_id, first, thrloc, tsession, &has_rows);
		switch (rv) {
			case BPTREE_OP_KEY_NOT_FOUND:
				DBUG_RETURN(HA_ERR_END_OF_FILE);
				break;
			case BPTREE_OP_KEY_FOUND:
				break;
			default:
				printf("Error on row prefetch %d\n", rv);
				DBUG_RETURN(-1);
				break;
		}
	}

	if (has_rows)
	{
		unpack_row_into_buffer(buf,thrloc->val_buf[thrloc->val_buf_pos]);
		thrloc->val_buf_pos++;
		DBUG_RETURN(0);
	} else
	{
		thrloc->val_buf_pos = 0;
		thrloc->val_buf_sz = 0;
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}
}

int ha_tapioca::prefetch_tapioca_rows(tapioca_bptree_id tbpt_id, bool first,
		tapioca_thrloc *thrloc, tapioca_table_session *tsession, bool *has_rows)
{
	DBUG_ENTER("ha_tapioca::prefetch_tapioca_rows");
	int ksize, vsize,  rv, mgets;
	int16_t keys_read = 0;
	*has_rows = false;
	bptree_mget_result *bmres, *cur;
	mget_result *mres;
	rv = 1;
	tapioca_handle *th = thrloc->th;
	uchar k[TAPIOCA_MAX_VALUE_SIZE];
	uchar v[TAPIOCA_MAX_VALUE_SIZE];
	uchar *kptr = write_tapioca_buffer_header(k);
	uchar *vptr = write_tapioca_buffer_header(v);
	uchar *pk_ptr;
	int *pk_size;
	if (tbpt_id == tsession->tbpt_ids[table->s->primary_key])
	{
		pk_ptr = k;
		pk_size = &ksize;
	} else
	{
		pk_ptr = v;
		pk_size = &vsize;
	}
	if (first)
	{
		rv = tapioca_bptree_index_first_no_key(th, tbpt_id);
		if (rv != BPTREE_OP_KEY_FOUND) goto prefetch_error;
	}

	rv = tapioca_bptree_index_next_mget(th, tbpt_id, &bmres, &keys_read);
	if (rv != BPTREE_OP_KEY_FOUND) goto prefetch_error;

	cur = bmres;
	for (int i =1; i <= keys_read; i++)
	{
		memcpy(kptr, cur->k, cur->ksize);
		memcpy(vptr, cur->v, cur->vsize);
		ksize = cur->ksize;
		vsize = cur->vsize;
		tapioca_mget(th, pk_ptr, *pk_size+get_tapioca_header_size());
		if(i < keys_read) cur = cur->next;
	}
	mres = tapioca_mget_commit(th);
	mgets = 0;

	while(mget_result_count(mres) > 0)
	{
		int bytes;
		bytes = mget_result_consume(mres, thrloc->val_buf[mgets]);
		mgets++;
	}
	assert(mgets == keys_read);

	thrloc->val_buf_sz = keys_read;
	thrloc->val_buf_pos = 0;
	// only if we have had no luck buffering new rows is this false
	*has_rows = (keys_read > 0);
	assert(is_thrloc_sane(thrloc));
	DBUG_RETURN(rv);

prefetch_error:
	thrloc->val_buf_sz = 0;
	thrloc->val_buf_pos = 0;
	*has_rows = false;
	DBUG_RETURN(rv);

}

/*@
 * Return the row for the given primary key
 * Expects *k to contain a valid key buffer and *rowbuf to be an empty area
 * to store the intermediate row
 * *buf should point to the buffer from the calling mysql function where the
 * unpacked row will go
 * If we store the row in the PK index, this will do a search; otherwise it
 * will be a direct get to tapioca
 */
int ha_tapioca::read_index_key(uchar *buf, uchar *k, uchar *rowbuf, int ksize)
{
	DBUG_ENTER("ha_tapioca::read_index_key");
	int rv, vsize;
	if (is_row_in_node)
	{
		// We want to skip the tapioca row header stuff
		assert(pk_tbpt_id != -1);
		rv = tapioca_bptree_search(thrloc->th, pk_tbpt_id,
				k+get_tapioca_header_size(),
			ksize - get_tapioca_header_size(), rowbuf, &vsize);
	}
	else
	{
		rv = tapioca_get(thrloc->th, k, ksize, rowbuf, TAPIOCA_MAX_VALUE_SIZE);
		if (rv == -1) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	rv = unpack_row_into_buffer(buf, rowbuf);
	DBUG_RETURN(rv);
}

tapioca_handle * ha_tapioca::get_current_tapioca_handle()
{
	DBUG_ENTER("ha_tapioca::get_current_tapioca_handle");
	assert(is_thrloc_sane(thrloc));
	DBUG_RETURN(thrloc->th);

}

// We'll leave this method unchanged for now and create a new buffered read
// method that will first be used by rnd_next()
int ha_tapioca::index_fetch(uchar *buf, bool first)
{
	DBUG_ENTER("ha_tapioca::index_fetch");
	assert(is_thrloc_sane(thrloc));
	// Just in case we somehow enter here again
	int ksize, vsize, pk_size, rv;
	uchar k[TAPIOCA_MAX_VALUE_SIZE];
	uchar v[TAPIOCA_MAX_VALUE_SIZE];
	uchar rowbuf[TAPIOCA_MAX_VALUE_SIZE];
	uchar *kptr = write_tapioca_buffer_header(k);
	uchar *vptr = write_tapioca_buffer_header(v);
	uchar *pk_ptr;
	tapioca_handle *th;
	int bps_id = active_index;
	// It seems that active_index 64 means "no" index; so we want the primary
	if (active_index == 64) bps_id = 0;

	tapioca_table_session *tsession;

	if (!(tsession = (tapioca_table_session *) my_hash_search(&thrloc->tsessions,
			(uchar*) full_table_name, strlen(full_table_name))))
	{
		printf("TAPIOCA: Exception in session lookup in idx_ftch\n");
		DBUG_RETURN(-1);
	}

	th = thrloc->th;
	tapioca_bptree_id tbpt_id = tsession->tbpt_ids[bps_id];

	if (first)
	{
		rv = tapioca_bptree_index_first(th, tbpt_id, kptr, &ksize, vptr, &vsize);
	}
	else
	{
		rv = tapioca_bptree_index_next(th, tbpt_id, kptr, &ksize, vptr, &vsize);
	}

	// If this is the primary key; the key buffer is the key itself;
	// otherwise it's pointed to in the value
	if (tbpt_id == tsession->tbpt_ids[table->s->primary_key])
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
		// FIXME DUH ! Only read the value as the row if this is the PK!
		if (is_row_in_node && (bps_id == 0))
		{
			rv = unpack_row_into_buffer(buf, vptr);
		}
		else
		{
			rv = read_index_key(buf, pk_ptr, rowbuf,
					pk_size + get_tapioca_header_size());
		}

		DBUG_RETURN(rv);
	}
	else if (rv == BPTREE_OP_EOF)
	{
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

	// TODO This is probably dangerous, do we need to do some kind of realloc
	// on this->ref?
	int pk_len;
	uchar *pk_ptr = ref;
	pk_ptr = write_tapioca_buffer_header(ref);
	construct_pk_buffer_from_row(pk_ptr, (uchar *) record, &pk_len);
	ref_length = pk_len + get_tapioca_header_size();
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
	uchar rowbuf[TAPIOCA_MAX_VALUE_SIZE];
	tapioca_bptree_id tbpt_id;

	rv = read_index_key(buf, pos, rowbuf, ref_length );

	DBUG_RETURN(rv);
}

int ha_tapioca::info(uint flag)
{
	// TODO Add appropriate information for optimizer 
	DBUG_ENTER("ha_tapioca::info");
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
	assert(is_thrloc_sane(thrloc));
	tapioca_table_session *tsession = (tapioca_table_session *) my_malloc(
			sizeof(tapioca_table_session), MYF(0));

	tsession->tbpt_ids = (tapioca_bptree_id *) my_malloc(
			sizeof(tapioca_bptree_id) * table->s->keys, MYF(0));
	assert (tsession != NULL);
	assert (tsession->tbpt_ids != NULL);

	// tbpt_id is now just a typdef of an int since we manage everything
	// internally inside tapioca
	tapioca_bptree_id *tbptr = tsession->tbpt_ids;

	KEY* key_info = table->key_info;
	for (int i = 0; i < table->s->keys; i++)
	{
		tapioca_bptree_info *bpt_info;
		String full_index_str(TAPIOCA_MAX_TABLE_NAME_LEN * 2);
		get_table_index_key(&full_index_str, full_table_name, key_info->name);
		const char *full_index_name = full_index_str.ptr();

		if (!(bpt_info = (tapioca_bptree_info *) my_hash_search(&tapioca_bptrees,
				(uchar*) full_index_name, strlen(full_index_name))))
		{
			printf("TAPIOCA: Couldn't find bpt meta info for %s\n",
					full_index_name);
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
		*tbptr = tapioca_bptree_initialize_bpt_session_no_commit(thrloc->th,
				bpt_info->bpt_id, BPTREE_OPEN_ONLY, new_bptree_exec_id);

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
	tapioca_thrloc *_thrloc = (tapioca_thrloc *) malloc(sizeof(tapioca_thrloc));
	thrloc = _thrloc;
	thrloc->accesses = 1;
//	tapioca_handle *th = init_tapioca_connection(&thrloc->node_id);
//	thrloc->th_enabled = 1;
	thrloc->th_enabled = 0;
	thrloc->tx_active = false;
	thrloc->num_tables_in_use = 0;
	thrloc->open_tables = 0;
	thrloc->val_buf_pos = -1;
	thrloc->val_buf_sz = -1;
	thrloc->should_not_be_written = 0xDEADBEEF;
	thrloc->tapioca_client_id = -1;
#ifdef BPTREE_BUFFERING
	for (int i = 0; i < TAPIOCA_MGET_BUFFER_SIZE; i++ )
	{
		bzero(thrloc->val_buf[i],TAPIOCA_MAX_VALUE_SIZE);
	}
#endif
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
		pk_tbpt_id = tsession->tbpt_ids[0];
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
			printf("TAPIOCA: Attempt to retrieve tsession failed in THD %p, "
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
			thrloc->tx_active = true;
		}
		else
		{
			trans_register_ha(thd, false, tapioca_hton);
			thrloc->tx_active = false;
		}

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
		if (thrloc->num_tables_in_use == 0)
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

				if (rv < 0) DBUG_RETURN(HA_ERR_TOO_MANY_CONCURRENT_TRXS);
				statistic_add(table->in_use->status_var.ha_savepoint_count, rv, &LOCK_status);
				statistic_increment( table->in_use->status_var.ha_savepoint_rollback_count, &LOCK_status);
			}

		}
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
 */

// TODO We need to do a better job of estimating this, because it is likely
// the cause for TPC-C queries that do not use more than the second level of an
// index
// FIXME If we have a statement like UPDATE .. WHERE k > 10; max_key will be
// NULL and this method will segfault!
ha_rows ha_tapioca::records_in_range(uint inx, key_range *min_key,
		key_range *max_key)
{
	DBUG_ENTER("ha_tapioca::records_in_range");

/*
	char minstr[500];
	char maxstr[500];
	memarea_as_string(minstr, (unsigned char *)min_key->key, min_key->length);
	memarea_as_string(maxstr, (unsigned char *)max_key->key, max_key->length);

	DBUG_PRINT("ha_tapioca", ("Called on idx %d with min key %s, len %d"
				" max key %s , len %d",inx, minstr, min_key->length,
				maxstr, max_key->length));
*/

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
	pthread_mutex_lock(&tapioca_mutex);
	rv = reload_tapioca_bptree_metadata();
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
					BPTREE_DEBUG_DUMP_RECURSIVE);
		}
		else if (check_opt->flags & T_QUICK)
		{
			printf("Seq. dump of idx %d name %s, %d parts, bpt_id %d",
					i,key_info->name, key_info->key_parts, *tbptr);
			rv = tapioca_bptree_debug(th, *tbptr,
					BPTREE_DEBUG_DUMP_SEQUENTIAL);
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
		DBUG_RETURN(reload_tapioca_bptree_metadata());
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
			int16_t is_pk = (i == table_arg->s->primary_key);
			rv = create_new_bpt_id(name, ki->name, is_pk);
			if (rv == -1) goto create_exception;

			ki++;
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
		const char *index_name, int16_t is_pk)
{
	DBUG_ENTER("ha_tapioca::create_new_bpt_id");
	int rv, mk = 0;
	int tries = 0, rv_p;
	const char* bpt_meta_key = TAPIOCA_BPTREE_META_KEY;
	void *buf;
	tapioca_bptree_id prev_bpt_id = -1, tbps_ignored;

	tapioca_bptree_info *bpt_info = (tapioca_bptree_info *) my_malloc(
			sizeof(tapioca_bptree_info), MYF(0));

	bpt_info->bpt_id = -1;
	bpt_info->is_pk = is_pk;
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

	// Make sure the metadata is created on tapioca for this table
	// Blow away whatever bpt_id might have existed there before;
	//	it will then be ready for when ::open is called
	tbps_ignored = tapioca_bptree_initialize_bpt_session(th_global,
			bpt_info->bpt_id, BPTREE_OPEN_OVERWRITE);
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
		tapioca_bptree_info *t = (tapioca_bptree_info *) my_hash_element(
				&tapioca_bptrees, i);
		printf("bpt_id %d, is_pk %d, name %s namelen %d active %d\n",
				t->bpt_id, t->is_pk, t->full_index_name,
				strlen(t->full_index_name), t->is_active);
	}
	fflush(stdout);

	mk = 4;
	int32_t bsize;
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

uchar * ha_tapioca::marshall_bptree_info(int32_t *bsize)
{
	DBUG_ENTER("ha_tapioca::marshall_bptree_info");
	int i;
	tpl_node *tn;
	tpl_bin tb_keys;
	tpl_bin tb_values;
	uchar *b;
	char *tpl_fmt_str = TAPIOCA_TPL_BPTREE_ID_MAP_FMT;

	tapioca_bptree_info m;
	tn = tpl_map(tpl_fmt_str, &m, TAPIOCA_MAX_TABLE_NAME_LEN * 2);

	//tpl_pack( tn, 0 ); // pack the non-array elements?
	for (i = 0; i < tapioca_bptrees.records; i++)
	{
		uchar *bpt_info = my_hash_element(&tapioca_bptrees, i);
		if (bpt_info == NULL)
			DBUG_RETURN(NULL);
		memcpy(&m, bpt_info, sizeof(tapioca_bptree_info));
		tpl_pack(tn, 1);
	}

	tpl_dump(tn, TPL_MEM, &b, bsize);
	tpl_free(tn);
	DBUG_RETURN(b);
}

tapioca_bptree_info *
unmarshall_bptree_info(const void *buf, uint16_t *num_bptrees)
{
	DBUG_ENTER("unmarshall_bptree_info");
	int i, rv, rv1, rv2, arr_sz;
	tpl_node *tn;
	tapioca_bptree_info m;

	char *tpl_fmt_str = TAPIOCA_TPL_BPTREE_ID_MAP_FMT;
	tn = tpl_map(tpl_fmt_str, &m, TAPIOCA_MAX_TABLE_NAME_LEN * 2);

	rv = tpl_load(tn, TPL_MEM | TPL_PREALLOCD | TPL_EXCESS_OK, buf,
			4096);
	if (rv < 0)
		DBUG_RETURN(NULL);
	arr_sz = tpl_Alen(tn, 1);
	DBUG_PRINT("ha_tapioca", ("Got bptree arr_sz of %d", arr_sz));
	tapioca_bptree_info *n = (tapioca_bptree_info *) malloc(
			sizeof(tapioca_bptree_info) * arr_sz);
	if (rv < 0)
		DBUG_RETURN(NULL);
	for (i = 0; i < arr_sz; i++)
	{
		rv1 = tpl_unpack(tn, 1);
		if (rv1 < 0)
			DBUG_RETURN(NULL);
		memcpy(&n[i], &m, sizeof(tapioca_bptree_info));
	}
	tpl_free(tn);
	*num_bptrees = arr_sz;
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
		tapioca_bptree_info *bpt_info =
				(tapioca_bptree_info *) my_hash_element(&tapioca_bptrees, i);
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
	int32_t bsize, attempts = 0;
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
	if (thrloc == NULL) return false;
	return
		(thrloc->val_buf_pos <= TAPIOCA_MGET_BUFFER_SIZE) &&
		(thrloc->val_buf_pos <= thrloc->val_buf_sz);
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
