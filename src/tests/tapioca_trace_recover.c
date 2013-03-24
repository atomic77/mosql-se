#include "tapioca.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>


#define TAPIOCA_ROW_PACKET_HEADER 0x3
#define BPTREE_META_NODE_PACKET_HEADER 0x4
#define BPTREE_NODE_PACKET_HEADER 0x5

typedef struct stats {
	int num_keys, tot_key_sz, tot_val_sz, commits , failed ;
	int dump_node_ids ;
	int row_keys, row_val_sz, btree_keys , btree_val_sz , meta_keys;
	struct timespec tm_st, tm_end;
} st_stats ;

void output_final_stats(st_stats *t);
void update_stats(unsigned char *k, int ksize, int vsize, st_stats *t);

void output_final_stats(st_stats *t) {
	fprintf(stderr, "\n");
	fprintf(stderr, "Total keys: %d\n", t->num_keys);
	fprintf(stderr, "Key size: %d\n", t->tot_key_sz);
	fprintf(stderr, "Val size: %d\n", t->tot_val_sz);
	if(t->row_keys > 0)
		fprintf(stderr, "Row keys: %d, total size %d, avg row_sz: %.1f  \n",
			t->row_keys, t->row_val_sz, (float)(t->row_val_sz/t->row_keys));
	if(t->btree_keys > 0)
	      fprintf(stderr, "B+Tree keys: %d, total size %d, avg node sz: %.1f \n",
			t->btree_keys, t->btree_val_sz, (float)(t->btree_val_sz / t->btree_keys));
	fprintf(stderr, "Meta keys: %d \n", t->meta_keys);
	fprintf(stderr, "Total commits: %d\n", t->commits);
	fprintf(stderr, "Failed commits: %d\n\n", t->failed);
//	fprintf(stderr, "Average commit size: %.1f \n",
//			(float)(t->tot_key_sz+t->tot_val_sz)/(t->commits));
}

void update_stats(unsigned char *k, int ksize, int vsize, st_stats *t) {

	if(*k ==  TAPIOCA_ROW_PACKET_HEADER) {
		t->row_keys++;
		t->row_val_sz+= vsize;
	} else if(*k == BPTREE_NODE_PACKET_HEADER) {
		t->btree_keys++;
		t->btree_val_sz += vsize;
	} else if (*k == BPTREE_META_NODE_PACKET_HEADER) {
		t->meta_keys++;
	} else {
//		fprintf(stderr, "Other key header found at len %d key %s\n", ksize, k);
	}
	t->num_keys ++;
	t->tot_key_sz+= ksize;
	t->tot_val_sz+= vsize;

	if ((t->num_keys) %  (50*1000) == 0) {
		clock_gettime(CLOCK_MONOTONIC, &t->tm_end);
		float secs = (t->tm_end.tv_sec-t->tm_st.tv_sec) +
				(t->tm_end.tv_nsec - t->tm_st.tv_nsec) / 1000000000.0;
		float drate = (t->tot_key_sz+t->tot_val_sz) / secs;
		float krate = t->num_keys / secs;
		drate = drate / (1024*1024);
		fprintf(stderr, "%d keys, %.1f MB k_sz, %.1f MB v_sz, lpos %d"
				" time %.1f Rate %.1f MB/s %.1f k/s\n",
				t->num_keys, (t->tot_key_sz/1024/1024.0), (t->tot_val_sz/1024/1024.0),
				secs,drate, krate);
	}

}
int main(int argc, char **argv) {
	int rv,rv2=123,ksize,vsize,i, comm_sz = 0, dirty = 0;
	int COMMIT_INTERVAL = 45000;
	st_stats t;
	bzero(&t, sizeof(st_stats));
	int16_t bpt_id;
	int64_t node_id;
	int keys_written = 0;

	unsigned char k[65000], v[65000];
	int port;
	const char *tracefile;
	FILE *fp;
	fpos_t pos, tmp_pos;
	if (argc < 4)
	{
		fprintf(stderr, "Usage %s <Host> <Port> <TraceFile> [CommitInt] \n", argv[0]);
		exit(-1);
	}
	port = atoi(argv[2]);
	tracefile = argv[3];
	if (argc == 5) COMMIT_INTERVAL = atoi(argv[4]);

	tapioca_handle *th = tapioca_open(argv[1],port);
	if (th == NULL) { fprintf(stderr, "Tapioca conn failed\n"); exit(-1); }

	fp = fopen(tracefile,"r");
	if (fp == NULL) {
		printf("Failed to open %s\n",tracefile);
		exit (-1);
	}
	printf("Opened file %s , fp %p \n", tracefile, fp);
	clock_gettime(CLOCK_MONOTONIC, &t.tm_st);
	while (fread(&ksize,sizeof(int),1,fp) != 0) {
		fread(k,ksize,1,fp);
		fread(&vsize,sizeof(int),1,fp);
		fread(v,vsize,1,fp);

		rv = tapioca_mput(th, k, ksize, v, vsize);
		dirty =1;
		keys_written++;
		if (rv > 50*1024 || keys_written >= 500) {
			rv2 = tapioca_mput_commit_retry(th, 10);
			if (rv2 < 0) goto err;
			dirty = 0;
			keys_written = 0;
		}

		update_stats(k,ksize,vsize,&t);

	}
	if (dirty)
	{
		rv2 = tapioca_mput_commit_retry(th, 10);
		if (rv2 < 0) goto err;
	}
	output_final_stats(&t);
	exit(0);
	err:
		fclose(fp);
		fprintf(stderr, "Error while loading (%d records loaded)\n", t.num_keys);
	        fprintf(stderr, "Current k/v sizes: %d %d rv, rv2: %d %d \n",
                		ksize, vsize, rv, rv2);
		return -1;
}

