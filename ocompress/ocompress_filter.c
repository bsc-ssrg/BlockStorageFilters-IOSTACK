/*
###########################################################################
#  (C) Copyright 2016 Barcelona Supercomputing Center                     #
#                     Centro Nacional de Supercomputacion                 #
#                                                                         #
#  This file is part of the IOSTACK Filter samples.                       #
#                                                                         #
#  Developer: Ramon Nou                                                   #
#                                                                         #
#                                                                         #
#  This package is free software; you can redistribute it and/or          #
#  modify it under the terms of the GNU Lesser General Public             #
#  License as published by the Free Software Foundation; either           #
#  version 3 of the License, or (at your option) any later version.       #
#                                                                         #
#  The IOSTACK Filter samples is distributed in the hope that it will     #
#  be useful, but WITHOUT ANY WARRANTY; without even the implied          #
#  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR                #
#  PURPOSE.  See the GNU Lesser General Public License for more           #
#  details.                                                               #
#                                                                         #
#  You should have received a copy of the GNU Lesser General Public       #
#  License; if not, write to the Free                                     #
#  Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.     #
#                                                                         #
###########################################################################
*/
#include <stdio.h>
#include "ocompress_filter.h"
#include "ocompress_defines.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "libtree.h"
#include "minilzo.h"
#include "zlib.h"
#include <pthread.h>
#include <sys/time.h> 


int g_flag = 0;
char *filter_name = "OCOMPRESS";
char *filename = "/tmp/prefetchlist.txt";

int count = 0;
unsigned long long number_elements = 0;
unsigned long long totalCompress = 0;
unsigned long long totalUncompress = 0;


//#define DEBUG
//
#define CACHE 1
//#define DELAYED 1         // It does not work some bug was introduced
//#define BUFFERED 1        // It does not work  some bug was introduced
#define DIRTY 1             // Stores Dirty Blocks information
//#define USEDIRTY 1          // Tries to reuse dirty blocks (use with CACHE only)
//#define BEST 1  // Selects the best compressor (time consuming as it tries all the compressors)
// 10000 Cached reads
#define IN_LEN      (4*1024ul)
#define OUT_LEN     (IN_LEN + IN_LEN / 16 + 64 + 3)
#define HEAP_ALLOC(var,size) \
    lzo_align_t __LZO_MMODEL var [ ((size) + (sizeof(lzo_align_t) - 1)) / sizeof(lzo_align_t) ]

#define MINCOMP 50  // minimum compressed block optimization

#define MAXINFLY 160
#define SIZECBUFFER 20
#define WAITTIME 20

void store_debug (char * line)
{
    int fd = open ("/tmp/debugprefetch", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}




// REDBLACK TREE
struct OffsetData
{
    long unsigned int key;  // ·OFFSET
    struct rbtree_node node;
};




int  KeyComp(const struct rbtree_node * a, const struct rbtree_node * b) {
    struct OffsetData *p = rbtree_container_of(a, struct OffsetData, node);
    struct OffsetData *q = rbtree_container_of(b, struct OffsetData, node);
    if (p->key > q->key) return 1;
    if (p->key < q->key) return -1;
    return 0;
}

struct rbtree indexTree;


#ifdef BUFFERED
struct PendingBlocks
{
    long unsigned int key;  // ·OFFSET
    char buffer[4096];             // Uncompressed content
    struct rbtree_node node;
};
struct rbtree bufferTree;

int  KeyCompBuffer(const struct rbtree_node * a, const struct rbtree_node * b) {
    struct PendingBlocks *p = rbtree_container_of(a, struct PendingBlocks, node);
    struct PendingBlocks *q = rbtree_container_of(b, struct PendingBlocks, node);
    if (p->key > q->key) return 1;
    if (p->key < q->key) return -1;
    return 0;
}
//char buffer[4096*MAXINFLY];  // Buffer to store elements  (only 512 blocks)
unsigned int numElements = 0;
//unsigned long long bufferedOffsets[MAXINFLY];  // Offset stored  (1 = Non assigned , 4096 aligned)
char compressedBufferWrite[4096*SIZECBUFFER];
#define BUFFEREDMODIFICATION 0
#define INPUTMODIFICATION 1
#define CLEARBUFFERED 2

bool obtainBufferedBlock (unsigned long long offset, void *buf, int operation, unsigned short cnt, unsigned long long internalOffset)
{


    struct PendingBlocks pb;
    pb.key = offset;

    struct rbtree_node * nodeR = rbtree_lookup (&pb.node, &bufferTree);

    if (nodeR)
    {
        struct PendingBlocks * pblks = rbtree_container_of (nodeR, struct PendingBlocks, node);
        // Exists
        if (operation == INPUTMODIFICATION)
            memcpy (buf, pblks->buffer+internalOffset, cnt);
        else if (operation == BUFFEREDMODIFICATION)
            memcpy(pblks->buffer+internalOffset, buf, cnt);
        else if (operation == CLEARBUFFERED)
        {
            // remove
            rbtree_remove (&pblks->node, &bufferTree );
        }
#ifdef DEBUG
        char linea[300];
        sprintf(linea,"obtainBufferedBlock True %llu %u\n", offset, cnt);
        store_debug (linea);
#endif
        return true;
    }


#ifdef DEBUG
    char linea[300];
    sprintf(linea,"obtainBufferedBlock False %llu %u\n", offset, cnt);
    store_debug (linea);
#endif
    return false;
}

#endif

pthread_t thread1, thread2;
pthread_mutex_t process_mutex, write_mutex;
pthread_mutexattr_t Attr;


pthread_cond_t process_cond;
bool GOOD = false;
//0172c6576c9a35bbfc65568ac37ef7b7


void modifyBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned char *compressedType, unsigned int fd, void *buf, bool check, bool occupied);
void modifyBlockBuffered (unsigned long long originalBlock, unsigned long long *lastPos, unsigned long long *initialPos, unsigned int fd, void * buf, bool check, bool occupied , bool last);




/*
int read (int fd, void * buf, size_t size)
{
     {
            char lin[1000];
            sprintf (lin, "DEVICEREAD %lu\n", size);
            store_debug (lin);
        }
        return read (fd,buf,size);
}
*/

unsigned int fdOther;

bool init = false;
bool end = false;

/* CACHED Variables */
unsigned long long last_block = 0;
#ifdef CACHE
struct CS {
    unsigned long long cachedOffset;
    unsigned short cachedSize;
    unsigned char  cachedType;
};

struct CS cachedStruct[DISKRECORDS];
bool cached = false;

unsigned long long last_dirty = 0;
struct CS cachedDIRTY[DIRTYBLOCKS];
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
    unsigned long long LBLOCK = TOTALINIT + PADDING;
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
    unsigned char CTYPE = EMPTY;

#ifdef CACHE
    last_block = LBLOCK;
    cached = true;
#endif
    for (unsigned long long i = 0; i < DISKRECORDS ; i++)
    {
        if (format) write(fd, &TMP, sizeof(unsigned long long));
        else read (fd, &TMP, sizeof(unsigned long long));
        if (format) write(fd, &SIZE, sizeof(unsigned short));
        else read (fd, &SIZE, sizeof(unsigned short));
        if (format) write(fd, &CTYPE, sizeof(unsigned char));
        else read (fd, &CTYPE, sizeof(unsigned char));
#ifdef CACHE
        cachedStruct[i].cachedOffset = TMP;
        cachedStruct[i].cachedSize = SIZE;
        cachedStruct[i].cachedType = CTYPE;
#endif
    }



    for (unsigned long long i = 0 ; i < DIRTYBLOCKS; i++)
    {
        if (format) write(fd, &TMP, sizeof(unsigned long long));
        else read (fd, &TMP, sizeof(unsigned long long));
        if (format) write(fd, &SIZE, sizeof(unsigned short));
        else read (fd, &SIZE, sizeof(unsigned short));
        if (format) write(fd, &CTYPE, sizeof(unsigned char));
        else read (fd, &CTYPE, sizeof(unsigned char));
    #ifdef CACHE
            cachedDIRTY[i].cachedOffset = TMP;
            cachedDIRTY[i].cachedSize = SIZE;
            cachedDIRTY[i].cachedType = CTYPE;
    #endif
    }

    

    #ifdef CACHE
    last_dirty = DSIZE;
    #endif
    store_debug ("Disk Initialized\n");
}


// Todo, the block should be 4K aligned. Check if it is guaranteed;
bool findBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned char * compressedType, unsigned int fd)
{

#ifdef CACHE

    *compressedBlock  = cachedStruct[originalBlock/4096].cachedOffset;
    *compressedSize = cachedStruct[originalBlock/4096].cachedSize;
    *compressedType = cachedStruct[originalBlock/4096].cachedType;

#else

    //lseek (fd, ( originalBlock / 4096 ) * SIZERECORD + HEADER , SEEK_SET);
    pread (fd, compressedBlock, sizeof(unsigned long long),( originalBlock / 4096 ) * SIZERECORD + HEADER);
    pread (fd, compressedSize, sizeof(unsigned short),( originalBlock / 4096 ) * SIZERECORD + HEADER + 8);
    pread (fd, compressedType, sizeof(unsigned char), (originalBlock / 4096) * SIZERECORD + HEADER + 8 + 2);
    #ifdef DEBUG
         {
            char lin[1000];
            sprintf (lin, "Find block %llu @ %llu %u %u\n", originalBlock, *compressedBlock, *compressedSize, *compressedType);
            store_debug (lin);
        }
    #endif

#endif

    if (*compressedType == EMPTY ) return false;

    return true;
}

void updateIndex (unsigned long long originalBlock, unsigned long long compressedBlock, unsigned short compressedSize, unsigned char compressedType, unsigned long long lBlock, unsigned int fd)
{
#ifdef CACHE
    if (lBlock != 0) last_block = lBlock;
    cachedStruct[originalBlock/4096].cachedOffset = compressedBlock;
    cachedStruct[originalBlock/4096].cachedSize = compressedSize;
    cachedStruct[originalBlock/4096].cachedType = compressedType;
#endif
#ifdef DELAYED
    // ADD a pending index
    unsigned long long index =  originalBlock / 4096;
    // Mutex
    

    // Store modified offset in RBTree
    struct OffsetData * offsetData = malloc (sizeof(struct OffsetData));
    offsetData->key = index;
    pthread_mutex_lock(&process_mutex);
    rbtree_insert(&offsetData->node, &indexTree);
    pthread_mutex_unlock (&process_mutex);
    //  pthread_cond_signal (&process_cond);
#else
  
    if (lBlock != 0) {
        pwrite (fd, &lBlock, sizeof(unsigned long long), 8 );
    }
   // lseek (fd, ( originalBlock / 4096 ) * SIZERECORD + HEADER , SEEK_SET);
    pwrite (fd, &compressedBlock, sizeof(unsigned long long), ( originalBlock / 4096 ) * SIZERECORD + HEADER);
    pwrite (fd, &compressedSize, sizeof(unsigned short),( originalBlock / 4096 ) * SIZERECORD + HEADER + 8);
    pwrite (fd, &compressedType, sizeof(unsigned char), (originalBlock / 4096) * SIZERECORD + HEADER + 10);
#ifdef DEBUG
    {
        char lin[1000];
        sprintf (lin, "Updated Index %llu @ %llu %u %u last : %llu\n", originalBlock, compressedBlock, compressedSize, compressedType, lBlock);
        store_debug (lin);
    }
#endif
#endif




}

#ifdef DELAYED
void *writeUpdate (void *arg)
{
    while (1)
    {
        



        //   store_debug ("Finish Batch\n");
#ifdef BUFFERED


        // memset(compressedBufferWrite,0,4096*SIZECBUFFER);
        unsigned long long lastPos = 0;
        unsigned long long initialPos = 0;

        pthread_mutex_lock (&write_mutex);
        struct rbtree_node * nodeR = rbtree_first (&bufferTree);
        pthread_mutex_unlock (&write_mutex);
        //  struct rbtree_node * nodeNext = rbtree_first (&bufferTree);

        while (nodeR)
        {

            struct PendingBlocks *fData2 = rbtree_container_of(nodeR, struct PendingBlocks, node);
            unsigned long long offset = fData2->key;

            
            modifyBlockBuffered (offset , &lastPos, &initialPos, fdOther, fData2->buffer, true, false ,false);
            pthread_mutex_lock (&write_mutex);
            nodeR = rbtree_next(nodeR);
            rbtree_remove(&fData2->node, &bufferTree);
            numElements--;
            pthread_mutex_unlock(&write_mutex);
            free (fData2);
        }

        // It should be a flush
        modifyBlockBuffered (1 , &lastPos, &initialPos, fdOther, NULL, true, false ,true);
      
#endif

        sleep (WAITTIME);

    }
    return NULL;


}
void *processUpdate (void *arg)
{


    while (1)
    {


        /*   struct timespec timeToWait;
           struct timeval now;

           gettimeofday(&now,NULL);
           timeToWait.tv_sec = now.tv_sec+10;
           timeToWait.tv_nsec = 0;


        pthread_cond_timedwait(&process_cond, &process_mutex, &timeToWait);
        */

        if (last_block != 0)
        {
           // lseek (fdOther, 8, SEEK_SET);
            pwrite (fdOther, &last_block, sizeof(unsigned long long),8);
        }

        pthread_mutex_lock (&process_mutex);
        struct rbtree_node * nodeR = rbtree_first (&indexTree);
        pthread_mutex_unlock(&process_mutex);
        // As we only remove here, we can unlock easily
        while (nodeR)
        {

            struct OffsetData *fData2 = rbtree_container_of(nodeR, struct OffsetData, node);
            unsigned long long offset = fData2->key;

            pthread_mutex_lock(&process_mutex);
            nodeR = rbtree_next(nodeR);
            rbtree_remove(&fData2->node, &indexTree);
            free (fData2);
            pthread_mutex_unlock(&process_mutex);
#ifdef DEBUG
            char lin[1000];
            sprintf (lin, "PUPDATE %llu %llu @ %u %u\n", *offset, cachedStruct[*offset].cachedOffset, cachedStruct[*offset].cachedSize,cachedStruct[*offset].cachedType );
            store_debug (lin);
#endif
           // lseek (fdOther, ( offset ) * SIZERECORD + HEADER, SEEK_SET);
            pwrite(fdOther, &cachedStruct[offset], SIZERECORD,( offset ) * SIZERECORD + HEADER);

            //RemoveElement (&pending_list, *offset);
            //RemoveHead (&pending_list);
        }
#ifdef DEBUG
        char lin[1000];
        sprintf (lin, "PUPDATE Finished\n");
        store_debug (lin);
#endif

        sleep (WAITTIME); // Batch 5 second
    }
    return NULL;
}
#endif

bool checkMagic (unsigned int fd)
{
    fdOther = fd;
    if (GOOD) return true;

    unsigned long long MAGIC;
   // lseek (fd, 0, SEEK_SET);
    pread (fd, &MAGIC, sizeof(unsigned long long),0);

#ifdef BUFFERED
    rbtree_init (&bufferTree,KeyCompBuffer, 0);

    /*  pthread_mutexattr_init(&Attr);
      pthread_mutexattr_settype(&Attr, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&process_mutex, &Attr);
    */
#endif

    if (MAGIC == MAGICCONST) {
        GOOD = true;

        initializeDisk (fd, false);


        return true;
    }
    else {
        initializeDisk (fd, true);
        GOOD = true;

    if (lzo_init() != LZO_E_OK)
    {
        printf("internal error - lzo_init() failed !!!\n");
        printf("(this usually indicates a compiler bug - try recompiling\nwithout optimizations, and enable '-DLZO_DEBUG' for diagnostics)\n");
        return 3;
    }

#ifdef DELAYED
        rbtree_init (&indexTree,KeyComp, 0);


        pthread_create(&thread1, NULL, processUpdate, NULL);
        pthread_create(&thread2, NULL, writeUpdate, NULL);
#endif
        return false;

    }

}


void putContent (unsigned long long compressedBlock, unsigned int compressedSize, unsigned int fd, void *buf)
{

   // lseek (fd, compressedBlock, SEEK_SET);
    pwrite (fd, buf, compressedSize, compressedBlock);
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
    //lseek (fd, 8, SEEK_SET); // Jump MAGIC number
    unsigned long long lBlock;
    pread (fd, &lBlock, sizeof (unsigned long long),8);
    return lBlock;
}


unsigned long long updateDirty ( unsigned long long offset, unsigned short size, unsigned int fd ) // Size will not be bigger than 4096 KB
{
  //  return 1;
    lseek (fd, 16, SEEK_SET);
    unsigned long long lDirty;
    read (fd, &lDirty, sizeof( unsigned long long));

    lseek (fd, HEADER + DISKRECORDS*SIZERECORD + lDirty * SIZERECORD + PADDING, SEEK_SET);

    write (fd, &offset, sizeof(unsigned long long));
    write (fd, &size, sizeof(unsigned short));


    // Type ommited

    lDirty = lDirty + 1; // A new dirty
    lseek (fd, 16, SEEK_SET);
    write (fd, &lDirty, sizeof (unsigned long long));

    // TODO : Use thread to update/write
    #ifdef CACHE
        cachedDIRTY[lDirty].cachedOffset = offset;
        cachedDIRTY[lDirty].cachedSize = size;
        last_dirty = lDirty;
    #endif

    return lDirty; //
}


bool lastDirty (unsigned long long *offset, unsigned short * size, unsigned int fd)
{
    unsigned long long lDirty;
    #ifdef CACHE
        if (last_dirty == 0 ) return false;
        cachedDIRTY[last_dirty-1].cachedOffset = *offset;
        cachedDIRTY[last_dirty-1].cachedSize = *size;
        return true;
    #else
    lseek (fd, 16, SEEK_SET);
    read (fd, &lDirty, sizeof( unsigned long long));

    if (lDirty > 0)
    {
        lseek (fd, HEADER + DISKRECORDS*SIZERECORD + (lDirty - 1) * SIZERECORD + PADDING, SEEK_SET); // last One

        read (fd, offset, sizeof(unsigned long long));
        read (fd, size, sizeof(unsigned short));

        return true;
    }
    else return false;

    #endif
}
#ifdef BUFFERED
// Not modified for type 
void modifyBlockBuffered (unsigned long long originalBlock, unsigned long long *lastPos, unsigned long long *initialPos, unsigned int fd, void * buf, bool check, bool occupied , bool last)
{
    // Find if it free

    if (originalBlock == 1 && last == true)
    {
        /*  {
            char lin[1000];
            sprintf (lin, "Storing Batch 1 %llu %llu \n", *initialPos , *lastPos);
            store_debug (lin);
          }*/
        // flush
        putContent(*initialPos, *lastPos-*initialPos, fd, compressedBufferWrite);
        *lastPos = 0;
        *initialPos = 0;
        return;
    }
    unsigned long long compressedBlock;
    unsigned short compressedSize;

    if ( check )
        occupied = findBlock (originalBlock, &compressedBlock, &compressedSize, fd);
    unsigned long long oldCompressedBlock = compressedBlock;

    unsigned long  newSize = OUT_LEN;//compressBound(BLOCKSIZE);
    unsigned char  compContent[5012];

    static HEAP_ALLOC(wrkmem, LZO1X_1_MEM_COMPRESS);
    // Compress the content
    lzo1x_1_compress(buf,BLOCKSIZE,compContent,&newSize,wrkmem);
    // Compress the content
   // compress2 (compContent,   &newSize , buf, BLOCKSIZE, COMPLEVEL);
    //realloc (compContent, newSize);
    totalCompress += newSize;
    totalUncompress += BLOCKSIZE;




    if (!occupied)
    {
        // Update Index


        unsigned long long lBlock = readLastBlockFree (fd);
        compressedBlock = lBlock;


        if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
        else
            lBlock += newSize;

        /* {
                 char lin[1000];
                 sprintf (lin, "Writing at %llu %u \n", compressedBlock , newSize);
                 store_debug (lin);
               }*/

        // Check if it is contiguous
        if (*lastPos != 0) // Initial
        {


            if (compressedBlock != *lastPos || ((*lastPos - *initialPos) + newSize + MINCOMP) > 4096*SIZECBUFFER )  // Or it does not fit
            {
                // Flush
                /*   {char lin[1000];
                   sprintf (lin, "Storing Batch 2 %llu %llu != %llu or %llu  \n", *initialPos , *lastPos, compressedBlock, ((*lastPos - *initialPos) + newSize + MINCOMP));
                   store_debug (lin);
                   }*/
                putContent(*initialPos, *lastPos-*initialPos, fd,compressedBufferWrite);
                *initialPos = compressedBlock;
                memcpy (compressedBufferWrite, compContent, newSize);
            } else

                memcpy (compressedBufferWrite+(*lastPos - *initialPos), compContent, newSize);


            *lastPos = lBlock;


        }
        else
        {
            *initialPos = compressedBlock;          // Inital position for the lseek write.
            *lastPos = lBlock;   // lastPos contains the last position
            memcpy (compressedBufferWrite, compContent, newSize);
        }



#ifdef DEBUG
        {
            char lin[1000];
            sprintf (lin, "modifyBlockBuffered FREE %llu %llu %lu stored at: %llu \n", originalBlock , lBlock, newSize, compressedBlock);
            store_debug (lin);
        }
#endif

        updateIndex (originalBlock, compressedBlock, newSize, lBlock ,fd);

        // Update Content

        if (last) {
            /*{
              char lin[1000];
              sprintf (lin, "Storing Batch LAST %llu %llu \n", *initialPos , *lastPos);
              store_debug (lin);
            }*/

            putContent(*initialPos, *lastPos-*initialPos,fd, compressedBufferWrite);
            *lastPos = 0;
            *initialPos = 0;
        }
        //putContent (*compressedBlock, newSize, fd, compContent);
        compressedSize = newSize;
       
    }
    else
    {


        if (newSize > MINCOMP && compressedSize < newSize)
        {
            // We need to put the content on another place
            unsigned long long lBlock = readLastBlockFree (fd);
            compressedBlock = lBlock;
            if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
            else
                lBlock += newSize;

#ifdef DEBUG
            {
                char lin[1000];
                sprintf (lin, "modifyBlockBuffered FREE 1xx %llu %llu %lu oldsize: %u stored at: %llu\n", originalBlock , lBlock, newSize, compressedSize, compressedBlock);
                store_debug (lin);
            }
#endif

            updateIndex (originalBlock, compressedBlock, newSize, lBlock ,fd);
            putContent (compressedBlock, newSize,fd, compContent);
           
            // Add dirty
            if (compressedSize < MINCOMP) compressedSize = MINCOMP;
#ifdef DIRTY
            unsigned long long num = updateDirty (oldCompressedBlock, compressedSize, fd); // We put a new dirty block on the list
#ifdef DEBUG
            char linea[300];
            sprintf(linea,"Dirty Blocks %llu\n", num );
            store_debug (linea);
#endif
#endif
            compressedSize = newSize;
        }
        else
        {
            // It fits
#ifdef DEBUG
            {
                char lin[1000];
                sprintf (lin, "modifyBlockBuffered FREE 2xx %llu @ %llu %lu < %u \n", originalBlock , compressedBlock, newSize, compressedSize);
                store_debug (lin);
            }
#endif
            // We substitute old block and update the size. We put the free bytes as a entry in the list.

            putContent (compressedBlock, newSize, fd, compContent);
           
            // Update the index without increasing the lBlock
            if (newSize != compressedSize)
            {
                updateIndex (originalBlock, compressedBlock, newSize, 0, fd);
            }
            // If we are MINCOMP does not update dirty
            if (newSize > MINCOMP && newSize != compressedSize)
            {
#ifdef DIRTY
                unsigned long long num = updateDirty (compressedBlock + newSize, compressedSize - newSize, fd); // We put a new dirty block on the list
                compressedSize = newSize;
#ifdef DEBUG
                char linea[300];
                sprintf(linea,"Dirty Blocks %llu\n", num );
                store_debug (linea);
#endif
#endif
            }


        }
    }
}
#endif
void modifyBlock (unsigned long long originalBlock, unsigned long long *compressedBlock, unsigned short *compressedSize, unsigned char *compressedType, unsigned int fd, void *buf, bool check, bool occupied)
{
    // Find if it free
    if ( check )
        occupied = findBlock (originalBlock, compressedBlock, compressedSize, compressedType, fd);
    unsigned long long oldCompressedBlock = *compressedBlock;

    unsigned long  newSize = OUT_LEN;//compressBound(BLOCKSIZE);
    unsigned long newSize2 = 0;
    unsigned char compContent[15012];
    unsigned char compContentZ[15012];
    static HEAP_ALLOC(wrkmem, LZO1X_1_MEM_COMPRESS);
    // Compress the content
    *compressedType = MINILZOP;
    lzo1x_1_compress(buf,BLOCKSIZE,compContent,&newSize,wrkmem);


    #ifdef BEST
        newSize2 = compressBound(BLOCKSIZE);
        // Compress the content 
        compress2 (compContentZ,   &newSize2 , buf, BLOCKSIZE, COMPLEVEL);

        if (newSize2 < newSize)
        {
            *compressedType = ZLIB;
            newSize = newSize2;
            memcpy (compContent, compContentZ, newSize);    // not very efficient, but it should affect only compression
        }

    #endif
    //compress2 (compContent,   &newSize , buf, BLOCKSIZE, COMPLEVEL);
    //realloc (compContent, newSize);
    // TODO : Here we should check all the compressors...

    // If the newSize is greater than BLOCKSIZE type = NOCOMP
 
    if (newSize >= BLOCKSIZE){


        // Try ZLIB
        #ifndef BEST
        newSize = compressBound(BLOCKSIZE);
        // Compress the content 
        compress2 (compContent,   &newSize , buf, BLOCKSIZE, COMPLEVEL);
        #endif
        if (newSize >= BLOCKSIZE)
        {
            *compressedType = NOCOMP;
            newSize = BLOCKSIZE;
            memcpy (compContent,buf,BLOCKSIZE);
        }
        else
        {
            *compressedType = ZLIB;
        }
    }



    totalCompress += newSize;
    totalUncompress += BLOCKSIZE;

    if (!occupied)
    {
        // Update Index


        unsigned long long lBlock = readLastBlockFree (fd);
        //lBlock = lBlock + (4096 - (lBlock % 4096)); // Padding

        *compressedBlock = lBlock;
        if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
        else
            lBlock += newSize;
#ifdef DEBUG
        {
            char lin[1000];
            sprintf (lin, "MODIFYBLOCK FREE %llu %llu %lu stored at: %llu %u\n", originalBlock , lBlock, newSize, *compressedBlock, *compressedType);
            store_debug (lin);
        }
#endif

        updateIndex (originalBlock, *compressedBlock, newSize, *compressedType, lBlock ,fd);

        // Update Content

        putContent (*compressedBlock, newSize, fd, compContent);
        *compressedSize = newSize;
        
    }
    else
    {


        if (newSize > MINCOMP && *compressedSize < newSize)
        {
            // We need to put the content on another place


            #ifdef USEDIRTY
                // Search dirty cache to find a suitable position
                unsigned long *index;  
                bool found = search_dirty (newSize, *index);
                /// TODO : XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                if ( found )
                {
                    // Update dirty

                    // Store data

                    // Update metadata
                }
            #endif



            unsigned long long lBlock = readLastBlockFree (fd);
            //    lBlock = lBlock + (4096 - (lBlock % 4096)); // Padding


            *compressedBlock = lBlock;
            if ( newSize < MINCOMP ) lBlock += MINCOMP; // Optimization
            else
                lBlock += newSize;

#ifdef DEBUG
            {
                char lin[1000];
                sprintf (lin, "MODIFYBLOCK FREE 1xx %llu %llu %lu oldsize: %u stored at: %llu %u\n", originalBlock , lBlock, newSize, *compressedSize, *compressedBlock, *compressedType);
                store_debug (lin);
            }
#endif

            updateIndex (originalBlock, *compressedBlock, newSize, *compressedType, lBlock ,fd);
            putContent (*compressedBlock, newSize,fd, compContent);
           
            // Add dirty
            if (*compressedSize < MINCOMP) *compressedSize = MINCOMP;
#ifdef DIRTY
            unsigned long long num = updateDirty (oldCompressedBlock, *compressedSize, fd); // We put a new dirty block on the list

#ifdef DEBUG

            char linea[300];
            sprintf(linea,"Dirty Blocks %llu\n", num );
            store_debug (linea);
#endif
#endif
            *compressedSize = newSize;
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
            updateIndex (originalBlock, *compressedBlock, newSize, *compressedType, 0, fd);
            
            // If we are MINCOMP does not update dirty
            if (newSize > MINCOMP && newSize != *compressedSize)
            {
                *compressedSize = newSize;
#ifdef DIRTY
                unsigned long long num = updateDirty (*compressedBlock + newSize, *compressedSize - newSize, fd); // We put a new dirty block on the list


#ifdef DEBUG
                char linea[300];
                sprintf(linea,"Dirty Blocks %llu\n", num );
                store_debug (linea);
#endif
#endif
            }


        }
    }
}





// We do the reads on the prefetch so in the read we do not do anything

void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{

}


// Offset is the offset from 0 - 4096 and size is the size
void uncompressBlock (unsigned long long compressedBlockOff, unsigned short compressedSize, void * buf, unsigned long long offset, unsigned short size, unsigned int fd, unsigned char compressedType)
{
    unsigned char decomp [4096];
    unsigned long nsize = 4096;
    unsigned char compressedBlock [5096];
    //lseek (fd, compressedBlockOff, SEEK_SET);
    pread (fd, compressedBlock, compressedSize,compressedBlockOff);
#ifdef DEBUG
    char linea[300];
    sprintf(linea,"Uncompressing Blocks %llu %llu %u\n", compressedBlockOff, offset, size);
    store_debug (linea);
#endif

    if (compressedType == EMPTY) return;


    if ( size == 4096 )
    {
        if (compressedType == MINILZOP)
            lzo1x_decompress(compressedBlock,compressedSize,buf,&nsize,NULL);
        if (compressedType == ZLIB)
            uncompress (buf, &nsize,compressedBlock, compressedSize);
        if (compressedType == NOCOMP)
            memcpy(buf, compressedBlock, size);
       // uncompress (buf, &nsize,compressedBlock, compressedSize);
    }
    else
    {
        if (compressedType == MINILZOP)
        {
            lzo1x_decompress(compressedBlock,compressedSize,decomp,&nsize,NULL);
            // TODO: CHECK was memcpy (buf,decomp+offset,size);
            memcpy (buf,decomp+offset,size);
        }

        if (compressedType == ZLIB)
        {
            uncompress (decomp, &nsize,compressedBlock, compressedSize);
            memcpy (buf,decomp+offset,size);
        }
        if (compressedType == NOCOMP)
            memcpy (buf, compressedBlock+offset,size);
       // uncompress (decomp, &nsize, compressedBlock, compressedSize);

    }
   // free (compressedBlock);
}



bool recover_one (void *buf, unsigned long cnt, unsigned long int offset, unsigned int fd)
{
    unsigned long long compressedBlock;
    unsigned short compressedSize;
    unsigned char compressedType;
    bool occupied = false;
#ifdef DELAYED
#ifdef BUFFERED
    pthread_mutex_lock (&write_mutex);
    // Check if we have in the buffered elements
    occupied = obtainBufferedBlock (offset, buf, INPUTMODIFICATION,BLOCKSIZE,0);

    pthread_mutex_unlock(&write_mutex);

#endif
#endif
    if (!occupied) {
        occupied = findBlock (offset, &compressedBlock, &compressedSize, &compressedType, fd);

        if (occupied)
        {
            /*  char lin [ 1000 ];
             sprintf (lin, "READ r found? %d  -> %lu %lu  @ %llu - %u \n", occupied, offset, cnt, compressedBlock, compressedSize);
             store_debug (lin); */
            uncompressBlock ( compressedBlock, compressedSize, buf, offset%4096, cnt ,fd, compressedType);
        }
        else memset (buf, 0 , cnt);
        return false;
    }

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
        unsigned char compressedType;
        bool occupied = false;


#ifdef DELAYED
#ifdef BUFFERED
        pthread_mutex_lock (&write_mutex);

        // Check if we have in the buffered elements
        occupied = obtainBufferedBlock (cutOffset * BLOCKSIZE, buf, INPUTMODIFICATION, cnt, intOffset);
        if (occupied) *doRead = false;

        pthread_mutex_unlock (&write_mutex);


#endif
#endif
        if(!occupied)
        {
            occupied = findBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, &compressedType, fd);


            if (occupied)
            {
                /*        sprintf (lin, "READ found? %d  -> %lu %lu [%llu]  @ %llu - %u \n", occupied, offset, cnt, cutOffset*BLOCKSIZE, compressedBlock, compressedSize);
                   store_debug (lin); */
                uncompressBlock (compressedBlock, compressedSize, buf, intOffset, cnt, fd, compressedType);
            }
            else memset(buf,0, cnt);

            *doRead = false;
        }
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


        // Read entire block
        char entireBuf [ 4096 ];
        unsigned long long cutOffset = offset / BLOCKSIZE;
        unsigned short intOffset = offset % BLOCKSIZE;
        unsigned long long compressedBlock = 0;
        unsigned short compressedSize = 0;
        unsigned char compressedType = EMPTY;
        bool occupied = false;
        bool buffered = false;

        // Check if the block exists (Fast check if CACHE active)
        occupied = findBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, &compressedType, fdOther);
#ifdef DELAYED
#ifdef BUFFERED
        pthread_mutex_lock (&write_mutex);

        // If exists search if it is buffered
        if (occupied)
            buffered = obtainBufferedBlock (cutOffset*BLOCKSIZE, entireBuf, INPUTMODIFICATION,BLOCKSIZE,0); // updates entirebuf

#endif
#endif

        // If not found set to 0 memory (it is not needed)
        if (!occupied)  ; //memset (entireBuf, 0, BLOCKSIZE);
        else if (!buffered)  // And if it is not buffered, but exists decompress to fill gaps
            uncompressBlock (compressedBlock, compressedSize, entireBuf, 0, BLOCKSIZE, fdOther, compressedType);


        memcpy (entireBuf+intOffset,buf,cnt);  // we are going to modify the contents of 4Kb block


#ifdef BUFFERED
#ifdef DELAYED      // Update the decompressed buffer
        bool found = obtainBufferedBlock (cutOffset*BLOCKSIZE, entireBuf, BUFFEREDMODIFICATION,BLOCKSIZE,0); // updates buffer


        if (!found && numElements != MAXINFLY)  // If it does not exists... we have to add a new element
        {

            struct PendingBlocks * pblks = malloc (sizeof(struct PendingBlocks));
            pblks->key = cutOffset*BLOCKSIZE;
            memcpy(&pblks->buffer, entireBuf,BLOCKSIZE);
            rbtree_insert(&pblks->node, &bufferTree);

            numElements++;
        }
        else modifyBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, &compressedType, fdOther,entireBuf, false, occupied); // We need to wait to clean the buffered elements
        pthread_mutex_unlock(&write_mutex);
#endif
#else
        modifyBlock (cutOffset*BLOCKSIZE, &compressedBlock, &compressedSize, &compressedType, fdOther, entireBuf, false, occupied);
#endif


    }
    else
        for (int i=0; i < cnt/BLOCKSIZE; i++)

        {
#ifdef DELAYED
#ifdef BUFFERED
            // Check if we have in the buffered elements
            pthread_mutex_lock(&write_mutex);
            obtainBufferedBlock (offset+i*BLOCKSIZE , NULL, CLEARBUFFERED,BLOCKSIZE,0); // updates buffer
            pthread_mutex_unlock(&write_mutex);
#endif
#endif
            unsigned long long compressedBlock;
            unsigned short compressedSize;
            unsigned char compressedType;
            modifyBlock  (offset+i*BLOCKSIZE, &compressedBlock, &compressedSize, &compressedType, fdOther, buf+i*BLOCKSIZE, true, false);
        }



}

