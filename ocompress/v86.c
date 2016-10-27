#include <stdio.h>
#include "ocompress_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "libtree.h"
#include "zlib.h"
#include <pthread.h>

int g_flag = 0;
char *filter_name = "OCOMPRESS";
char *filename = "/tmp/prefetchlist.txt";

int count = 0;
unsigned long long number_elements = 0;
unsigned long long totalCompress = 0;
unsigned long long totalUncompress = 0;


//#define DEBUG 

#define CACHE 1
#define DELAYED 1

#define NUM_PREFETCH 150000
// 10000 Cached reads
#define GZIP_ENCODING   16
#define CHUNK       40960
#define BLOCKSIZE   4096
#define COMPLEVEL 1

#define MAGICCONST 0x01663
#define DISKRECORDS 5200000 // 20 GB of UNCOMPRESSED SPACE
#define DIRTYBLOCKS 100000   // 10000 compressed blocks that we can have dirty, and if we hit 10000 we put a reordering process.  // TODO Place to optimize
#define SIZERECORD 10 // 10 bytes, one unsigned long long plus a unsigned short
#define HEADER 24 // 8 MAGIC 8 LBLOCK 8 DIRTY
#define MINCOMP 150  // minimum compressed block optimization


pthread_t thread1;
pthread_mutex_t process_mutex, tree_mutex;
pthread_cond_t process_cond;
bool GOOD = false;
//0172c6576c9a35bbfc65568ac37ef7b7
void store_debug (char * line)
{
    int fd = open ("/tmp/debugprefetch", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}



unsigned int fdOther;

bool init = false;
bool end = false;

/* CACHED Variables */
unsigned long long last_block = 0;
#ifdef CACHE
unsigned long long *cachedOffset;
unsigned short *cachedSize;
bool cached = false;
#endif

#ifdef DELAYED
struct Node {
    void  *data;  // Store the offsets
    struct Node* next;
    struct Node* prev;
};

struct Node * pending_list;

//Creates a new Node and returns pointer to it.
struct Node* GetNewNode(void *x) {
    struct Node* newNode
        = (struct Node*)malloc(sizeof(struct Node));
    newNode->data = x;
    newNode->prev = NULL;
    newNode->next = NULL;
    return newNode;
}

//Inserts a Node at head of doubly linked list
void InsertAtHead(struct Node **head, void *x) {
    struct Node* newNode = GetNewNode(x);
    if(*head == NULL) {
        *head = newNode;
        return;
    }
    (*head)->prev = newNode;
    newNode->next = *head;
    *head = newNode;
}

//Inserts a Node at tail of Doubly linked list
void InsertAtTail(struct Node **head, void *x) {
    struct Node* temp = *head;
    struct Node* newNode = GetNewNode(x);
    if(*head == NULL) {
        *head = newNode;
        return;
    }
    while(temp->next != NULL) temp = temp->next; // Go To last Node
    temp->next = newNode;
    newNode->prev = temp;
}

void RemoveHead (struct Node **head)
{
 struct Node* temp = *head;
 if ( temp->next != NULL)
  temp->next->prev = NULL;
  *head = temp->next;
   free (temp);

}

void RemoveElement(struct Node **head, unsigned long long offset)
{
    struct Node* temp = *head;
    while(temp != NULL) {
        if ( *((unsigned long long*)temp->data) == offset && temp != *head)
        {
            // remove element
            if (temp->next != NULL)
                temp->next->prev = temp->prev;


            temp->prev->next = temp->next;
            free (temp->data);
            free (temp);
            break;
        }
        else if (*((unsigned long long*)temp->data) == offset && temp == *head)
        {
            if ( temp->next != NULL)
                temp->next->prev = NULL;
            *head = temp->next;
            free (temp->data);
            free (temp);
            break;
        }
        temp = temp->next;

    }
}


void RemoveAll(struct Node **head)
{
    struct Node* temp = *head;
    while(temp != NULL) {
        struct Node * next = temp->next;
        if (temp->data) {
            //char line[100];
            //sprintf (line,"Removing %llu %p\n",*(unsigned long long*)(temp->data->key), temp->data);
            //store_debug(line);
        } else store_debug ("FAIL\n");
       
        free (temp->data);  // Free OffsetData
        free (temp); // Free Node
        temp = next;
    }
    *head = NULL;
}


#endif

char *get_name()
{
    return filter_name;
}

void print_bind_msg(char *msg)
{
    printf("%s Output OCompresss \n", msg);
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




void initializeDisk (unsigned int fd, bool format)
{
    // Write MAGIC
    unsigned long long MAGIC = MAGICCONST;
    
    lseek (fd,0, SEEK_SET);
    if (format) write (fd, &MAGIC, sizeof(unsigned long long));
    else read (fd, &MAGIC, sizeof(unsigned long long));
    // Write LAST USED BLOCK
    unsigned long long LBLOCK = DISKRECORDS * SIZERECORD + DIRTYBLOCKS * SIZERECORD +  HEADER;
    #ifdef DEBUG 
        {
            char lin[1000];
            sprintf (lin, "LBLOCK EQUAL TO %llu\n", LBLOCK);
            store_debug (lin);
        }
    #endif

    if (format) write (fd, &LBLOCK, sizeof(unsigned long long));
    else read (fd, &LBLOCK, sizeof(unsigned long long));

    // Write number of dirty blocks
    unsigned long long DSIZE = 0;
    if (format) write (fd, &DSIZE, sizeof(unsigned long long));
    else read (fd, &DSIZE, sizeof(unsigned long long));

    unsigned long long TMP = 0;
    unsigned short SIZE = 0;    // Empty record
    #ifdef CACHE
    cachedOffset = malloc (DISKRECORDS*sizeof(unsigned long long));
    cachedSize = malloc (DISKRECORDS*sizeof(unsigned short));
    last_block = LBLOCK;
    cached = true;
    #endif
    for (unsigned long long i = 0; i < DISKRECORDS ; i++)
    {
        if (format) write(fd, &TMP, sizeof(unsigned long long));
        else read (fd, &TMP, sizeof(unsigned long long));
        if (format) write(fd, &SIZE, sizeof(unsigned short));
        else read (fd, &SIZE, sizeof(unsigned short));
        #ifdef CACHE
        cachedOffset[i] = TMP;
        cachedSize[i] = SIZE;
        #endif
    }



    for (unsigned long long i = 0 ; i < DIRTYBLOCKS; i++)
    {
        if (format) write(fd, &TMP, sizeof(unsigned long long));
        if (format) write(fd, &SIZE, sizeof(unsigned short));
    }
    //store_debug ("Disk Initialized\n");
}


// Todo, the block should be 4K aligned. Check if it is guaranteed;
bool findBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned int fd)
{

    #ifdef CACHE
       /* if (!cached)
        {
            cachedOffset = malloc (DISKRECORDS*sizeof(unsigned long long));
            cachedSize = malloc (DISKRECORDS*sizeof(unsigned short));
            cached = true;
            for (unsigned long long i = 0; i < DISKRECORDS ; i++)
             {
            read(fd, &cachedOffset[i], sizeof(unsigned long long));
            read(fd, &cachedSize[i], sizeof(unsigned short));
     
            }

        } */

            *compressedBlock  = cachedOffset[originalBlock/4096];
            *compressedSize = cachedSize[originalBlock/4096];

    #else

        lseek (fd, ( originalBlock / 4096 ) * SIZERECORD + HEADER , SEEK_SET);
        read (fd, compressedBlock, sizeof(unsigned long long));
        read (fd, compressedSize, sizeof(unsigned short));

    #endif
    
    if (*compressedSize == 0 ) return false;
    #ifdef DEBUG 
        else {
              char lin[1000];
              sprintf (lin, "Find block %llu @ %llu %u\n", originalBlock, *compressedBlock, *compressedSize);
              store_debug (lin);
        }
    #endif
    return true;
}

void updateIndex (unsigned long long originalBlock, unsigned long long compressedBlock, unsigned short compressedSize, unsigned long long lBlock, unsigned int fd)
{
    #ifdef CACHE
        if (lBlock != 0) last_block = lBlock;
        cachedOffset[originalBlock/4096] = compressedBlock;
        cachedSize[originalBlock/4096] = compressedSize;
    #endif
    #ifdef DELAYED
        // ADD a pending index
        unsigned long long * index = malloc (sizeof(unsigned long long));
        *index = originalBlock / 4096;
         // Mutex
        pthread_mutex_lock(&process_mutex);
        InsertAtHead(&pending_list, index); 
       
        pthread_mutex_unlock (&process_mutex);
        pthread_cond_signal (&process_cond);
    #else
    lseek (fd, 8, SEEK_SET);
    if (lBlock != 0) { write (fd, &lBlock, sizeof(unsigned long long)); }
    lseek (fd, ( originalBlock / 4096 ) * SIZERECORD + HEADER , SEEK_SET);
    write (fd, &compressedBlock, sizeof(unsigned long long));
    write (fd, &compressedSize, sizeof(unsigned short));
    #endif

   

    #ifdef DEBUG
    {
          char lin[1000];
          sprintf (lin, "Updated Index %llu @ %llu %u last : %llu\n", originalBlock, compressedBlock, compressedSize, lBlock);
          store_debug (lin);
            }
    #endif
}

#ifdef DELAYED
void *processUpdate (void *arg)
{
    
    
    while (1)
    {
        pthread_mutex_lock (&process_mutex);
        while (pending_list == NULL) {
            pthread_cond_wait(&process_cond, &process_mutex); 
        }
        //int pending = 0;
        while (pending_list != NULL)
        {
            unsigned long long *offset = pending_list->data;
            unsigned long long compressedOffset = cachedOffset[*offset];
            unsigned short compressedSize = cachedSize[*offset];

 /*char lin[1000];
          sprintf (lin, "PUPDATE %llu %llu @ %llu\n", *offset, compressedOffset, compressedSize);
          store_debug (lin);
*/
            lseek (fdOther, ( *offset ) * SIZERECORD + HEADER , SEEK_SET);
            write (fdOther, &compressedOffset , sizeof(unsigned long long));
            write (fdOther, &compressedSize, sizeof(unsigned short));
          
            RemoveElement (&pending_list, *offset);    
          //   store_debug ("Stored\n");  
        }
        lseek (fdOther, 8, SEEK_SET);
        write (fdOther, &last_block, sizeof(unsigned long long)); 
      //   store_debug ("Finish Batch\n");
        pthread_mutex_unlock(&process_mutex);
        sleep (5); // Batch 5 second
    }
    return NULL; 
}
#endif

bool checkMagic (unsigned int fd)
{
    fdOther = fd;
    if (GOOD) return true;
    unsigned long long MAGIC;
    lseek (fd, 0, SEEK_SET);
    read (fd, &MAGIC, sizeof(unsigned long long));

   

    if (MAGIC == MAGICCONST) { GOOD = true; 

        initializeDisk (fd, false);


        return true; }
    else { 
        initializeDisk (fd, true);
        GOOD = true;

         #ifdef DELAYED
         pthread_create(&thread1, NULL, processUpdate, NULL);
        #endif
        return false;

    }

}


void putContent (unsigned long long compressedBlock, unsigned int compressedSize, unsigned int fd, void *buf)
{

    lseek (fd, compressedBlock, SEEK_SET);
    write (fd, buf, compressedSize);
    #ifdef DEBUG 
    {
        char lin[1000];
        sprintf (lin, "putContent %llu @ %u\n", compressedBlock, compressedSize);
        store_debug (lin);
    }
    #endif
}


unsigned long long readLastBlockFree (unsigned int fd)
{
    #ifdef CACHE
    if (last_block != 0) return last_block;
    #endif
    lseek (fd, 8, SEEK_SET); // Jump MAGIC number
    unsigned long long lBlock;
    read (fd, &lBlock, sizeof (unsigned long long));
    return lBlock;
}


unsigned long long updateDirty ( unsigned long long offset, unsigned short size, unsigned int fd ) // Size will not be bigger than 4096 KB
{
    return 1;
    lseek (fd, 16, SEEK_SET);
    unsigned long long lDirty;
    read (fd, &lDirty, sizeof( unsigned long long));

    lseek (fd, HEADER + DISKRECORDS*SIZERECORD + lDirty * SIZERECORD, SEEK_SET);

    write (fd, &offset, sizeof(unsigned long long));
    write (fd, &size, sizeof(unsigned short));

    lDirty = lDirty + 1; // A new dirty
    lseek (fd, 16, SEEK_SET);
    write (fd, &lDirty, sizeof (unsigned long long));


    return lDirty; // 
}


bool lastDirty (unsigned long long *offset, unsigned short * size, unsigned int fd)
{
    lseek (fd, 16, SEEK_SET);
    unsigned long long lDirty;
    read (fd, &lDirty, sizeof( unsigned long long));

    if (lDirty > 0)
    {
        lseek (fd, HEADER + DISKRECORDS*SIZERECORD + (lDirty - 1) * SIZERECORD, SEEK_SET); // last One

        write (fd, offset, sizeof(unsigned long long));
        write (fd, size, sizeof(unsigned short));

        return true;
    }
    else return false;
}


void modifyBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned int fd, void *buf, bool check, bool occupied)
{
    // Find if it free
    if ( check ) 
        occupied = findBlock (originalBlock, compressedBlock, compressedSize, fd);
    unsigned long long oldCompressedBlock = *compressedBlock;

    unsigned long  newSize = compressBound(BLOCKSIZE);
    unsigned char * compContent = malloc (newSize);

    // Compress the content 

    compress2 (compContent,   &newSize , buf, BLOCKSIZE, COMPLEVEL);
    //realloc (compContent, newSize);
    totalCompress += newSize;
    totalUncompress += BLOCKSIZE;

    if (!occupied)
    {       
        // Update Index

       
        unsigned long long lBlock = readLastBlockFree (fd);
        *compressedBlock = lBlock;
        if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
        else
        lBlock += newSize;
        #ifdef DEBUG 
        {
          char lin[1000];
          sprintf (lin, "MODIFYBLOCK FREE %llu %llu %lu stored at: %llu \n", originalBlock , lBlock, newSize, *compressedBlock);
          store_debug (lin);
        }
        #endif

        updateIndex (originalBlock, *compressedBlock, newSize, lBlock ,fd);

        // Update Content

        putContent (*compressedBlock, newSize, fd, compContent);

        free (compContent);
    }
    else
    {
        

        if (newSize > MINCOMP && *compressedSize < newSize)
        {
            // We need to put the content on another place
            unsigned long long lBlock = readLastBlockFree (fd);
             *compressedBlock = lBlock;
             if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
            else
            lBlock += newSize;
           
            #ifdef DEBUG 
            {
              char lin[1000];
              sprintf (lin, "MODIFYBLOCK FREE 1xx %llu %llu %lu oldsize: %u stored at: %llu\n", originalBlock , lBlock, newSize, *compressedSize, *compressedBlock);
              store_debug (lin);
            }
            #endif

            updateIndex (originalBlock, *compressedBlock, newSize, lBlock ,fd);
            putContent (*compressedBlock, newSize,fd, compContent);
            free (compContent);
            // Add dirty
            if (*compressedSize < MINCOMP) *compressedSize = MINCOMP;   
            unsigned long long num = updateDirty (oldCompressedBlock, *compressedSize, fd); // We put a new dirty block on the list
            #ifdef DEBUG 
            char linea[300];
            sprintf(linea,"Dirty Blocks %llu\n", num );
            store_debug (linea); 
            #endif
        }
        else
        {
            // It fits
            #ifdef DEBUG 
            {
              char lin[1000];
              sprintf (lin, "MODIFYBLOCK FREE 2xx %llu @ %llu %lu < %u \n", originalBlock , *compressedBlock, newSize, *compressedSize);
              store_debug (lin);
            }
            #endif
            // We substitute old block and update the size. We put the free bytes as a entry in the list.
            
        putContent (*compressedBlock, newSize, fd, compContent);
            free (compContent);
            // Update the index without increasing the lBlock
            if (newSize != *compressedSize)
            {
                updateIndex (originalBlock, *compressedBlock, newSize, 0, fd);
            }
            // If we are MINCOMP does not update dirty
            if (newSize > MINCOMP && newSize != *compressedSize)
            {
            unsigned long long num = updateDirty (*compressedBlock + newSize, *compressedSize - newSize, fd); // We put a new dirty block on the list
            #ifdef DEBUG 
            char linea[300];
            sprintf(linea,"Dirty Blocks %llu\n", num );
            store_debug (linea);
             #endif
            }
           

        }
    }
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


// Offset is the offset from 0 - 4096 and size is the size
void uncompressBlock (unsigned long long compressedBlockOff, unsigned short compressedSize, void * buf, unsigned long long offset, unsigned short size, unsigned int fd)
{
    unsigned char decomp [4096];
    unsigned long nsize = 4096;
    unsigned char * compressedBlock = malloc (compressedSize);
    lseek (fd, compressedBlockOff, SEEK_SET);
    read (fd, compressedBlock, compressedSize);

    if ( size == 4096 )
    {
    uncompress (buf, &nsize,compressedBlock, compressedSize);
    }
    else
    {
        uncompress (decomp, &nsize, compressedBlock, compressedSize);
        memcpy (buf,decomp+offset,size);
    }
    free (compressedBlock);
}



bool recover_one (void *buf, unsigned long cnt, unsigned long int offset, unsigned int fd)
{
    unsigned long long compressedBlock;
    unsigned short compressedSize;

    bool occupied = findBlock (offset, &compressedBlock, &compressedSize, fd);
   
    if (occupied)
    {
   /*  char lin [ 1000 ];
    sprintf (lin, "READ r found? %d  -> %lu %lu  @ %llu - %u \n", occupied, offset, cnt, compressedBlock, compressedSize);
    store_debug (lin); */
    uncompressBlock ( compressedBlock, compressedSize, buf, offset%4096, cnt ,fd);
    }
    else memset (buf, 0 , cnt);
    return false;
}

/* Prefetch a number of blocks, and give one of them if available*/
int pre_read(void* buf, unsigned long cnt, unsigned long int offset, unsigned int fd, bool *doRead)
{

    checkMagic (fd);

   #ifdef DEBUG 
    char lin [ 1000 ];
   
    {
    sprintf (lin, "READ %lu %lu\n", offset, cnt);
    store_debug (lin);
    }
    #endif
   

    if (cnt < BLOCKSIZE) {

    
        unsigned long long cutOffset = offset / BLOCKSIZE;
        unsigned short intOffset = offset % BLOCKSIZE;
        unsigned long long compressedBlock;
        unsigned short compressedSize;
        bool occupied = findBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, fd);

        if (occupied)
        {
         /*        sprintf (lin, "READ found? %d  -> %lu %lu [%llu]  @ %llu - %u \n", occupied, offset, cnt, cutOffset*BLOCKSIZE, compressedBlock, compressedSize);
            store_debug (lin); */
            uncompressBlock (compressedBlock, compressedSize, buf, intOffset, cnt, fd);
        }
        else memset(buf,0, cnt);

        *doRead = false;
    
    }
    else
    // Look for block asked
    for (int i=0; i < cnt/BLOCKSIZE; i++)
    {
        
       *doRead = recover_one (buf+i*BLOCKSIZE, BLOCKSIZE, offset+i*BLOCKSIZE, fd);
     
    }
  

    return 0;
}


// 
void write_xform( void* buf, unsigned long cnt, unsigned long int offset, bool *doWrite)
{
    //char * mybuf = malloc(cnt);
    //recover_one(mybuf,cnt,offset);
    //free (mybuf);
    //store_debug("Write\n");
    *doWrite = false;
    // TODO : It will fail if the cnt is not the same....
    #ifdef DEBUG 
    char lin [ 1000 ];

    {   
        sprintf (lin, "WRITE %lu %lu\n", offset, cnt);
        store_debug (lin);
    }
    #endif
 if (cnt < BLOCKSIZE) 
    {


    //char lin [ 1000 ];
    //sprintf (lin, "WRITE  %lu %lu\n", offset, cnt);
    //store_debug (lin);


    // Read entire block
    char entireBuf [ 4096 ];
    unsigned long long cutOffset = offset / BLOCKSIZE;
    unsigned short intOffset = offset % BLOCKSIZE;
    unsigned long long compressedBlock = 0;
    unsigned short compressedSize = 0;
    bool occupied = findBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, fdOther);

    if (!occupied) memset (entireBuf, 0, BLOCKSIZE);
    else 
        uncompressBlock (compressedBlock, compressedSize, entireBuf, 0, BLOCKSIZE, fdOther);

 
  /*  sprintf (lin, "WRITE PARTIAL %lu %lu | INTOFF: %u\n", offset, cnt, intOffset);
    store_debug (lin);
*/
    memcpy (entireBuf+intOffset,buf,cnt);  // we are going to modify the contents of 4Kb block

    // uncompress (if not empty)

    // Modify required blocks


    // Modifyblock
//void modifyBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned int fd, void *buf)
    modifyBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, fdOther, entireBuf, false, occupied);
        

    }
    else
  for (int i=0; i < cnt/BLOCKSIZE; i++)
    
    {
        unsigned long long compressedBlock;
        unsigned short compressedSize;
        modifyBlock  (offset+i*BLOCKSIZE, &compressedBlock, &compressedSize, fdOther, buf+i*BLOCKSIZE, true, false);
    }

   

}


/* Sample 


unsigned char *compressed = { 0 }, *uncompressed = { 0 };
    unsigned int compressedLen = 0, uncompressedLen = 0;
 
    unsigned char data2compress[] = "...A long character buffer...\0";
 
    if ((compressedLen = CompressBuffer(&compressed, data2compress, strlen(data2compress))) > 0){
 
        if ((uncompressedLen = UncompressBuffer(&uncompressed, compressed, compressedLen)) > 0){
            printf("Original data: %s\n\n", uncompressed);
            printf("Before compression: %d, After compression: %d\n", strlen(data2compress), compressedLen);
        }
    }


    */