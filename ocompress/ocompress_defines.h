

#define CHUNK       40960
#define BLOCKSIZE   4096
#define COMPLEVEL 1

#define MAGICCONST 0x01663
#define DISKRECORDS 5200000 // 20 GB of UNCOMPRESSED SPACE
#define DIRTYBLOCKS 100000   // 10000 compressed blocks that we can have dirty, and if we hit 10000 we put a reordering process.  // TODO Place to optimize
#define SIZERECORD 11 // 10 bytes, one unsigned long long plus a unsigned short plus a unsigned char
#define HEADER 24 // 8 MAGIC 8 LBLOCK 8 DIRTY

#define TOTALINIT ((DISKRECORDS+DIRTYBLOCKS)*SIZERECORD+HEADER)
#define PADDING (512 - TOTALINIT % 512)

#define MINCOMP 50  // minimum compressed block optimization


// Status for RECORD
#define EMPTY 0
#define MINILZOP 1
#define NOCOMP 2
#define ZLIB 3