/* Copyright (c) 2003, 2010 Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/** @file ha_tapioca.h

 @brief
 The ha_tapioca engine is a mysql interface to the Tapioca database developed
 at the University of Lugano, Switzerland

 @note
 Please read ha_tapioca.cc before reading this file.

 @see
 /sql/handler.h and /storage/tapioca/ha_tapioca.cc
 */

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

extern "C"
{
#include "tapioca_util.h"
#include <msgpack.h>
#include <uuid/uuid.h>
}


#include <mysql_version.h>
#define MYSQL_SERVER 

#if MYSQL_VERSION_ID>=50515
#include "sql_class.h"
#include "sql_priv.h"
// #include "sql_array.h"
#elif MYSQL_VERSION_ID>50100
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "../mysql_priv.h"
#endif

//#define BPTREE_BUFFERING
#define DEFENSIVE_MODE
// On write do we first check if the key exists?
//#define DEFENSIVE_INSERT
#define TAPIOCA_MAX_NODES_PER_MYSQL 20
// Number of thread-local slots of memory we create for storage engine ops
#define MOSQL_NUM_MEM_SLOTS 10 
#define MOSQL_IMPLICIT_PK_NAME "IMPLICIT_PK"
//#if MYSQL_VERSION_ID>=50500
//#include "sql_priv.h"
//#include "my_global.h"
//#include "handler.h"
//#include "probes_mysql.h"
//#include "sql_class.h"                          // SSV
//#include "sql_table.h"
//#else
//#include "mysql_priv.h"
//#endif

//#include <mysql/plugin.h>


#if MYSQL_VERSION_ID>=50515
#define TABLE_ARG	TABLE_SHARE
class THD;
#elif MYSQL_VERSION_ID>50100
#define TABLE_ARG	st_table_share
#else
#define TABLE_ARG	st_table
#endif


#if MYSQL_VERSION_ID>=50120
typedef uchar byte;
#endif


typedef struct st_tapioca_share
{
	char *table_name;
	char data_file_name[FN_REFLEN];
	uint table_name_length, use_count;
	pthread_mutex_t mutex;
	THR_LOCK lock;
} TAPIOCA_SHARE;

typedef struct st_tapioca_thrloc
{
	tapioca_bptree_id *tbpt_ids;
	tapioca_handle *th;
	int node_id; // 0-based index of which tapioca node we are connected to
	int accesses;
	bool tx_active;
	bool tx_cant_commit; // used in some cases where we should not commit
	int num_tables_in_use;
	int open_tables;
	int th_enabled;
	HASH tsessions;
	int should_not_be_written;
	int tapioca_client_id;

} tapioca_thrloc;

typedef struct st_tapioca_table_session
{
	char full_table_name[TAPIOCA_MAX_TABLE_NAME_LEN];
	tapioca_bptree_id *tbpt_ids; // should be a bps for each key in the table
	//tapioca_bptree_id tbpt_ids[MAX_KEY]; // should be a bps for each key in the table
} tapioca_table_session;


typedef struct st_tapioca_node_config
{
	char address[128];
	int port;
	int mysql_instance_num;
} tapioca_node_config;

int ha_tapioca_commit(handlerton *hton, THD *thd, bool all);
int ha_tapioca_rollback(handlerton *hton, THD *thd, bool all);
void get_table_index_key(String *s, const char *table_name, const char *index_name);
tapioca_bptree_info *
unmarshall_bptree_info(const char *buf, size_t sz, uint16_t *num_bptrees);
inline int get_tapioca_header_size();
uint32_t get_next_bptree_execution_id(int node);
/** @brief
 Class definition for the storage engine
 */
class ha_tapioca: public handler
{
	THR_LOCK_DATA lock;
	TAPIOCA_SHARE *share;
	// Pointer to thread local storage for the current
	tapioca_thrloc *thrloc;
	int rows_written;
	int write_rows;
	int32_t current_auto_inc;
	int32_t tapioca_table_id;
	char full_table_name[TAPIOCA_MAX_TABLE_NAME_LEN];
	int handler_tapioca_client_id;
	tapioca_bptree_id pk_tbpt_id; // Primary key tbpt_id for this table
	bool handler_opened ; // did this object ever pass through ::open?!
	bool is_row_in_node; // Does this table store the row in the bnode?
//	bool tapioca_write_opened;
	uchar tmp_row_buf[TAPIOCA_MAX_VALUE_SIZE]; // a place for weary data to rest


private:
    int init_tapioca_writer();
    tapioca_handle *init_tapioca_connection(int *node_id);
    int get_tapioca_table_id(tapioca_handle *th);
    int unpack_row_into_buffer_packed(uchar *buf, uchar *v);
    int unpack_row_into_buffer(uchar *buf, uchar *v);
    uchar *write_tapioca_buffer_header(uchar *buf);
    int get_row_by_key(uchar *buf, uchar *k);
    inline int get_pk_length();
    inline int get_key_level(const uchar *key, int len);
    int create_new_bpt_id(const char *table_name, const char *index_name,
                          TABLE *table_arg, int idx);
    int index_fetch(uchar *buf, bool first);
    int index_fetch_buffered(uchar *buf, bool first);
    tapioca_table_session *initialize_new_bptree_thread_data();
    void *initialize_thread_local_space();
    uchar *marshall_bptree_info(size_t *buf_sz);
    int create_or_set_thrloc(THD *thd);
    int create_or_set_tsession(THD *thd);
    int prefetch_tapioca_rows(tapioca_bptree_id tbpt_id, bool first,
                              tapioca_thrloc *thrloc, 
			      tapioca_table_session *tsession, bool *has_rows);
    tapioca_handle * get_current_tapioca_handle();
    uchar * construct_tapioca_row_buffer_packed(const uchar *buf, size_t * buf_sz);
    uchar * construct_tapioca_row_buffer(const uchar *buf, size_t * buf_sz);
    uchar * construct_tapioca_key_buffer(const uchar *key, uint key_len, uint idx,
                                         size_t *buf_sz, bool incl_header);
    uchar * construct_idx_buffer_from_row(const uchar *buf, size_t *buf_sz, int idx,
                                          bool incl_header);
    int update_indexes(const uchar *old_data, const uchar *new_data);
    int insert_to_index(const uchar *buf, int idx, uchar *row, size_t row_sz);
    int delete_from_index(const uchar *buf, int idx);

    inline uchar * get_next_mem_slot();

    tapioca_bptree_id get_tbpt_id_for_idx(int idx);

    inline bool is_index_buffer_exact_match(uint index, key_part_map keypart_map);

    inline int is_field_null(Field *field, const uchar *buf);
    inline int is_autoinc_needed(Field *field, const uchar *buf);
    inline bool table_has_pk() ;
    void handle_varchar(KEY_PART_INFO *key_part, uchar **k, const uchar *buf);
	int get_max_row_len();
public:
    ha_tapioca(handlerton *hton, TABLE_SHARE *table_arg);
    ~ha_tapioca()
    {
    }

    const char *table_type() const
    {
        return "MoSQL";
    }

    const char *index_type(uint inx)
    {
        return "BTREE";
    }

    const char **bas_ext() const;
    ulonglong table_flags() const
    {
        return(
		  HA_NULL_IN_KEY |
//		  HA_CAN_INDEX_BLOBS |
		  //HA_FAST_KEY_READ | // this causes a weird problem in sorting
//		  HA_CAN_SQL_HANDLER |
		  HA_PRIMARY_KEY_REQUIRED_FOR_POSITION 
		  | HA_REQUIRES_KEY_COLUMNS_FOR_DELETE
		  | HA_REC_NOT_IN_SEQ
		  | HA_PRIMARY_KEY_REQUIRED_FOR_DELETE 
		  | HA_PRIMARY_KEY_IN_READ_INDEX 
		  | HA_ANY_INDEX_MAY_BE_UNIQUE 
		  | HA_NO_PREFIX_CHAR_KEYS
//		  HA_NO_AUTO_INCREMENT   // stable
//		  HA_BINLOG_ROW_CAPABLE
//		  HA_CAN_GEOMETRY |
//		  HA_PARTIAL_COLUMN_READ
		  | HA_TABLE_SCAN_ON_INDEX  // for ORDER BY?
		  );
    }

    ulong index_flags(uint idx, uint part, bool all_parts) const
    {
        return (HA_READ_NEXT 
			| HA_READ_ORDER 
			| HA_READ_RANGE
			| HA_KEYREAD_ONLY);
    }

    uint max_supported_record_length() const
    {
        return HA_MAX_REC_LENGTH;
    }

    uint max_supported_keys() const
    {
        return 9;
    }

    uint max_supported_key_parts() const
    {
        return 9;
    }

    uint max_supported_key_length() const
    {
        return 1024;
    }

    virtual bool primary_key_is_clustered()
    {
        return TRUE;
    }

    virtual double scan_time()
    {
        return (double)(((((stats.records + stats.deleted))))) / 1.0 + 10;
    }

    virtual double read_time(uint, uint, ha_rows rows)
    {
        return (double)((((rows)))) / 20.0 + 1;
    }

    int open(const char *name, int mode, uint test_if_locked);
    int close(void);
    int write_row(uchar *buf);
    int write_all_indexes(uchar *buf, uchar *row, size_t row_sz);
    int update_row(const uchar *old_data, uchar *new_data);
    int delete_row(const uchar *buf);
    int index_next(uchar *buf);
    int index_prev(uchar *buf);
    int index_first(uchar *buf);
    int index_last(uchar *buf);
    int index_read(uchar *buf, const uchar *key, uint key_len,
    		enum ha_rkey_function find_flag);
	//int index_read_idx_map(uchar *buf, uint index, const uchar *key,
     //                             key_part_map keypart_map,
      //                            enum ha_rkey_function find_flag);
	//int index_read_idx(uchar* buf, uint keynr, const uchar* key, uint key_len,
	//		enum ha_rkey_function find_flag);
	int rnd_init(bool scan);
	int rnd_end();
	int rnd_next(uchar *buf);
	int rnd_pos(uchar *buf, uchar *pos);
	void position(const uchar *record);
	int info(uint);
	int extra(enum ha_extra_function operation);
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int delete_all_rows(void);
	ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
	int rename_table(const char *from, const char *to);
	int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
			enum thr_lock_type lock_type);
	int delete_table(const char *name);
	int repair(THD* thd, HA_CHECK_OPT* check_opt);
	int analyze(THD* thd, HA_CHECK_OPT* check_opt);
	int check(THD* thd, HA_CHECK_OPT* check_opt);

};

