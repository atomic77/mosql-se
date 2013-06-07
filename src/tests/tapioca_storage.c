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

#include <tapioca/tapioca.h>
#include <tapioca/tapioca_btree.h>
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

