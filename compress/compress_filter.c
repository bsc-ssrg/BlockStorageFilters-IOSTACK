// Sample filter: bitrev. Reverses each byte in input buffer (in place) providing mirror image of source byte.
#include <stdio.h>
#include "compress_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "libtree.h"
#include "minilzo.h"

int g_flag = 0;
char *filter_name = "COMPRESS";
char *filename = "/tmp/prefetchlist.txt";

int count = 0;
unsigned long long number_elements = 0;
unsigned long long totalCompress = 0;
unsigned long long totalUncompress = 0;
#define NUM_PREFETCH 2750000
// 10000 Cached reads
#define GZIP_ENCODING   16
#define CHUNK       40960
#define BLOCKSIZE   4096

#define IN_LEN      (4*1024ul)
#define OUT_LEN     (IN_LEN + IN_LEN / 16 + 64 + 3)
#define HEAP_ALLOC(var,size) \
    lzo_align_t __LZO_MMODEL var [ ((size) + (sizeof(lzo_align_t) - 1)) / sizeof(lzo_align_t) ]

static HEAP_ALLOC(wrkmem, LZO1X_1_MEM_COMPRESS);

void store_debug (char * line)
{
    int fd = open ("/tmp/debugprefetch", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}



FILE *fd_p = NULL;

bool init = false;
bool end = false;

struct PrefetchData
{
    long unsigned int key;  // ·OFFSET 
    void * content; // Compressed content
    long unsigned int cmpsize;
    struct rbtree_node node;
};


int  KeyComp(const struct rbtree_node * a, const struct rbtree_node * b) {
    struct PrefetchData *p = rbtree_container_of(a, struct PrefetchData, node);
    struct PrefetchData *q = rbtree_container_of(b, struct PrefetchData, node);
    if (p->key > q->key) return 1;
    if (p->key < q->key) return -1;
    return 0;
}

struct rbtree tree;


char *get_name()
{
    return filter_name;
}

void print_bind_msg(char *msg)
{
    printf("%s Prefetch Filter 2\n", msg);
    return;
}

int get_flag()
{
    return g_flag;
}

void set_flag(int flag)
{
    g_flag = flag;
    return;
}



// We do the reads on the prefetch so in the read we do not do anything 

void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{   
  /*  if(cnt < BLOCKSIZE) return 0;   // Only prefetch > 4096 blocks

    for (int i=0; i < cnt/BLOCKSIZE; i++)
    {
        struct PrefetchData * prefetchdata = malloc (sizeof(struct PrefetchData));
        prefetchdata->key = malloc (100);
        sprintf (prefetchdata->key,"%lu %lu",offset+i*BLOCKSIZE, BLOCKSIZE);  // TODO: Optimize offset / cnt
        prefetchdata->cmpsize = compressBound(BLOCKSIZE);
        prefetchdata->content = malloc (prefetchdata->cmpsize);

        compress2 (prefetchdata->content,   &prefetchdata->cmpsize , buf, BLOCKSIZE, 9);
        //prefetchdata->cmpsize = CompressBuffer(&prefetchdata->content, buf, cnt);
       // char linea[300];
        //sprintf(linea,"After comp %s - or: %d - com: %d\n", prefetchdata->key, cnt, prefetchdata->cmpsize );
        realloc (prefetchdata->content, prefetchdata->cmpsize);
        //store_debug(linea);

        rbtree_insert(&prefetchdata->node, &tree);
    }

    if (cnt/BLOCKSIZE * cnt < cnt) store_debug("LeftOver\n"); */
}


/* Read block  and compress it, put it on buf , we do not delete blocks but we have a limit*/
bool prefetch_one (unsigned int fd,void *buf, unsigned long cnt, unsigned long int offset)
{

   
    lseek (fd, offset, SEEK_SET);
    read (fd, buf, cnt);
    number_elements++;
    if (number_elements < NUM_PREFETCH)
    {
        struct PrefetchData * prefetchdata = malloc (sizeof(struct PrefetchData));
        prefetchdata->key = offset;
      //  prefetchdata->cmpsize = compressBound(BLOCKSIZE);
       // prefetchdata->content = malloc (prefetchdata->cmpsize);


      
            unsigned char compContent[15012];
    // Compress the content
        lzo1x_1_compress(buf,cnt,compContent,&prefetchdata->cmpsize,wrkmem);
        
        if (prefetchdata->cmpsize < 4096)
        {
            prefetchdata->content = malloc (prefetchdata->cmpsize);
        
            memcpy (prefetchdata->content,compContent,prefetchdata->cmpsize);
        } else
        {
            prefetchdata->cmpsize = BLOCKSIZE;
            prefetchdata->content = malloc (prefetchdata->cmpsize);
            memcpy(prefetchdata->content, buf, BLOCKSIZE);
        }
      //  compress2 (prefetchdata->content,   &prefetchdata->cmpsize , buf, BLOCKSIZE, 1);
        totalCompress += prefetchdata->cmpsize;
        totalUncompress += cnt;
       // char lin [ 1000 ];
       // sprintf (lin, "Compress %lu %f %lu %f \n", prefetchdata->cmpsize, 1.0 - (prefetchdata->cmpsize / 4096.0 ) , number_elements , 1.0 - ((float)totalCompress / (float)totalUncompress ) );
       // store_debug (lin);

     // realloc (prefetchdata->content, prefetchdata->cmpsize);

        rbtree_insert(&prefetchdata->node, &tree);
    }
    return true;
}



bool recover_one (void *buf, unsigned long cnt, unsigned long int offset)
{
    struct PrefetchData checkprefetch;
    checkprefetch.key = offset;
    //sprintf (checkprefetch.key,"%lu %lu",offset, cnt);


    struct rbtree_node * node_p = rbtree_lookup (&checkprefetch.node, &tree);
    if (node_p)
    {

        struct PrefetchData * pData = rbtree_container_of (node_p, struct PrefetchData, node);

     //   uncompress ( buf, &cnt,pData->content,   pData->cmpsize );

        if (pData->cmpsize < 4096)
         lzo1x_decompress(pData->content, pData->cmpsize ,buf,&cnt,NULL);
        else memcpy (buf,pData->content, BLOCKSIZE);

       // unsigned int size = UncompressBuffer(&buf, pData->content, pData->cmpsize, cnt);
        //char linea[200];
     //sprintf(linea,"After ucomp %s - or: %d - com: %d\n", checkprefetch.key, cnt, pData->cmpsize );
        //store_debug(linea);
       // memcpy (buf, pData->content,cnt);
        // decompress
        //rbtree_remove(&pData->node,&tree);
        //free(pData->content);
        //free(pData->key);
        //number_elements--;
        //free(checkprefetch.key);
        return false;
    }
    else
    {
        //free (checkprefetch.key);
        return true; // we have to read
    }
}

/* Prefetch a number of blocks, and give one of them if available*/
int pre_read(void* buf, unsigned long cnt, unsigned long int offset, unsigned int fd, bool *doRead)
{

    // The shmemory is opened
    if (init == false)
    {
        rbtree_init (&tree,KeyComp, 0);
        fd_p = fopen (filename, "r");
          
        if (lzo_init() != LZO_E_OK)
        {
            printf("internal error - lzo_init() failed !!!\n");
            printf("(this usually indicates a compiler bug - try recompiling\nwithout optimizations, and enable '-DLZO_DEBUG' for diagnostics)\n");
            return 3;
        }
        init = true;

    }


   /* if ( !end && number_elements == NUM_PREFETCH)
    {
        struct rbtree_node * node_p = rbtree_first (&tree);
        struct PrefetchData * pData = rbtree_container_of (node_p, struct PrefetchData, node);
        rbtree_remove(&pData->node,&tree);
        free(pData->content);
        free(pData->key);
        number_elements--;
    } */

  /*  while (!end && number_elements < NUM_PREFETCH)
    {
        end = prefetch_one(fd);
        if ( !end )
            number_elements++;
    }
*/
  //  char lin [ 1000 ];
   // sprintf (lin, "leftover read %lu %lu\n", cnt, cnt/BLOCKSIZE);

    if (cnt < BLOCKSIZE) {*doRead = true; return 0;}
    else
    // Look for block asked
    for (int i=0; i < cnt/BLOCKSIZE; i++)
    
    {
        *doRead = recover_one (buf+i*BLOCKSIZE, BLOCKSIZE, offset+i*BLOCKSIZE);
        if (*doRead == true)
        {
            // We have to read
            prefetch_one(fd, buf+i*BLOCKSIZE, BLOCKSIZE, offset+i*BLOCKSIZE);
            *doRead = false;
        }
    }
  //  if (cnt/BLOCKSIZE * cnt < cnt) store_debug(lin);
    return 0;
}


// 
void write_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    //char * mybuf = malloc(cnt);
    //recover_one(mybuf,cnt,offset);
    //free (mybuf);
    //store_debug("Write\n");

    // TODO : It will fail if the cnt is not the same....
 if (cnt < BLOCKSIZE) return;
  for (int i=0; i < cnt/BLOCKSIZE; i++)
    
    {
        struct PrefetchData checkprefetch;
        checkprefetch.key = offset+i*BLOCKSIZE;
        //sprintf (checkprefetch.key,"%lu %lu",offset+i*BLOCKSIZE, BLOCKSIZE);


        struct rbtree_node * node_p = rbtree_lookup (&checkprefetch.node, &tree);
        if (node_p) 
        {
            struct PrefetchData * pData = rbtree_container_of (node_p, struct PrefetchData, node);

            rbtree_remove(&pData->node,&tree);
            free(pData->content);
            //free(pData->key);
            number_elements--;
          //  free(checkprefetch.key);
        }
}
 if (cnt/BLOCKSIZE * cnt < cnt) store_debug("LeftOver write\n");


}

