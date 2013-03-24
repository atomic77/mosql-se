#include "tapioca.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
	int rv,ksize,vsize,i,succ_put = 0, val;
	int COMMIT_INTERVAL = 100;
	char *k, *v;
	int keys;
	if (argc < 5)
	{
		printf("Usage %s <Keys> <KSize> <VSize> <Val> <Commit Interval>\n", argv[0]);
		exit(-1);
	}
	keys = atoi(argv[1]);
	ksize = atoi(argv[2]);
	vsize = atoi(argv[3]);
	val = atoi(argv[4]);
	COMMIT_INTERVAL = atoi(argv[5]);
	k = malloc(ksize);
	v = malloc(vsize);
	memset(k,'K',ksize);
	memset(v,val,vsize);

	printf("Total effective storage: %.3f MiB\n",
			((float)((ksize+vsize)*keys)/(1024*1024)));

	tapioca_handle *th = tapioca_open("127.0.0.1",5555);
	if (th == NULL) { printf("Tapioca cxn failed\n"); exit(-1); }
	for (i = 1; i <= keys; i++ ){
		rv = sprintf(k, "%d", i);
		k[rv] = 'K'; // get rid of null terminator
		tapioca_put(th,k, ksize, v, vsize);
		if (i % COMMIT_INTERVAL == 0) {
			rv = tapioca_commit(th);
			if (rv >= 0) succ_put += COMMIT_INTERVAL;
		}
		if (i % 100 == 0) {
			printf(".");
			fflush(stdout);
		}
		if (i % 5000 == 0) printf (" %d\n",i);
	}

	printf("\nSuccessful puts %d of %d\n", succ_put, keys);

}

