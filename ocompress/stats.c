#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "ocompress_defines.h"
// Should be equal to ocompress filter
#define DEBUG 0



int main (int argc, char ** argv)
{
	char diskdevice[100];
	unsigned long long types[4];
	types[EMPTY] = 0;
	types[MINILZOP] = 0;
	types[NOCOMP] = 0;
	types[ZLIB] = 0;
	
	if (argc > 1 ) { strcpy(diskdevice, argv[1]); printf ("Stats for disk : %s\n", diskdevice); }
	 
	// Test 1 : Write at different zones a complete aligned block
	int fd = open (diskdevice,O_RDONLY);
	

	unsigned long long DRECORD;
	double CSIZE = 0.0;
	double RSIZE = 0.0;
	double DSIZE = 0.0;
	unsigned long long lDirty;
	for (int i = 0 ; i < DISKRECORDS; i++)
	{
		unsigned long long compressedBlock;
		unsigned short compressedSize;
		unsigned char compressedType;

	    lseek (fd, ( i ) * SIZERECORD + HEADER , SEEK_SET);
	    read (fd, &compressedBlock, sizeof(unsigned long long));
	    read (fd, &compressedSize, sizeof(unsigned short));
	 	read (fd, &compressedType, sizeof(unsigned char));
	 	types[compressedType]++;
	    if (compressedSize != 0 ) 
	    {
	    	CSIZE += compressedSize;
	    	RSIZE += BLOCKSIZE;
	    }
	}
	
	

	lseek (fd, 16, SEEK_SET);
	read (fd, &lDirty, sizeof (unsigned long long));
	for (int i = 0; i < lDirty; i++)
	{


		unsigned long long compressedBlock;
		unsigned short compressedSize;
		unsigned char compressedType;
		lseek (fd, (i) * SIZERECORD + DISKRECORDS * SIZERECORD + HEADER, SEEK_SET);
		read (fd, &compressedBlock, sizeof(unsigned long long));
		read (fd, &compressedSize, sizeof(unsigned short));
		read (fd, &compressedType, sizeof(unsigned char));
		DSIZE += compressedSize;
	}

	
	printf ("Output compress filter Stats : \n");
	printf ("Compressed Data : %lf MB Real Data : %lf MB Ratio : %lf\n", CSIZE/(1024.0*1024.0), RSIZE/(1024.0*1024.0), (1.0 - CSIZE / RSIZE) * 100.0);
	printf ("Dirty Blocks: %llu (Size : %lf MB)\n",lDirty,DSIZE/(1024.0*1024.0));
	printf ("Compressed Types: EMPTY %llu, MINILZOP %llu, NOCOMP %llu, ZLIB %llu\n", types[EMPTY], types[MINILZOP], types[NOCOMP], types[ZLIB]);
}
