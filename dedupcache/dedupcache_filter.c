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
#include "dedupcache_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>

#include <sys/types.h>

#include "libtree.h"
//#include <openssl/md5.h>

#include <pthread.h>

int g_flag = 0;
char *filter_name = "DEDUPCACHE";
#define BLOCKSIZE 4096
#define MAXSIZE 100000
// 100000 = 400 MB
//

int count = 0;
int init = 0;
struct rbtree tree;
struct rbtree offsettree;

pthread_t thread1;
pthread_mutex_t process_mutex, tree_mutex;
pthread_cond_t process_cond;




struct Node;
struct DedupData;
struct OffsetData;


struct DedupData  
{
    char * key;  // Memory content
//    char * pointer; // Memory content
    unsigned long counter; // Number of references
    struct Node * head;  // List of references (offsets)
    struct rbtree_node node;
};


struct Node {
    void  *data;  // Store the offsets
    struct Node* next;
    struct Node* prev;
};



struct OffsetData
{
    unsigned long long key; // offset
    struct DedupData *realdata;
    struct rbtree_node node;

};

struct Node * pending_list;

void store_debug (char * line)
{
    int fd = open ("/tmp/debugdedup", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}



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

void RemoveElement(struct Node **head, void * offset)
{
    struct Node* temp = *head;
    while(temp != NULL) {
        if ( temp->data == offset && temp != *head)
        {
            // remove element
            if (temp->next != NULL)
                temp->next->prev = temp->prev;


            temp->prev->next = temp->next;
            free (temp);
            break;
        }
        else if ( temp->data == offset && temp == *head)
        {
            if ( temp->next != NULL)
                temp->next->prev = NULL;
            *head = temp->next;
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
        rbtree_remove(&((struct OffsetData*)temp->data)->node, &offsettree);
        free (temp->data);  // Free OffsetData
        free (temp); // Free Node
        temp = next;
    }
    *head = NULL;
}


unsigned int numElements = 0;

void KeyDest(void* a) {
    free (a);
}



int  KeyComp(const struct rbtree_node * a, const struct rbtree_node * b) {
    struct DedupData *p = rbtree_container_of(a, struct DedupData, node);
    struct DedupData *q = rbtree_container_of(b, struct DedupData, node);
    int comp = memcmp (p->key,q->key,BLOCKSIZE);
//    int comp = strcmp (p->key,q->key);
    if (comp > 0) return 1;
    if (comp < 0) return -1;
    return 0;
}


int  OffsetComp(const struct rbtree_node * a, const struct rbtree_node * b) {

    struct OffsetData *p = rbtree_container_of(a, struct OffsetData, node);
    struct OffsetData *q = rbtree_container_of(b, struct OffsetData, node);


    if (p->key < q->key) return -1;
    if (p->key > q->key) return 1;
    return 0;
}


char *get_name()
{
    return filter_name;
}

void print_bind_msg(char *msg)
{
    printf("%s DedupCache Filter \n", msg);
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


void remove_block(unsigned long int tempoffset)
{
    if (init == 0) {
        rbtree_init (&tree,KeyComp, 0);
        rbtree_init (&offsettree,OffsetComp,0);
       
        init = 1;
    }


    struct OffsetData off;
    off.key =  tempoffset;

    struct rbtree_node *data = rbtree_lookup (&off.node,&offsettree);


    if (data)
    {
        struct OffsetData * fOff = rbtree_container_of (data, struct OffsetData, node);
        struct DedupData * fData = fOff->realdata;


        if ( fData->counter > 1)
        {
            fData->counter--;
            RemoveElement(&fData->head, fOff);	// Check!
            free (fOff);
            rbtree_remove(data,&offsettree);
        }
        else {
            RemoveAll(&fData->head); // Check
            //free (datainfo->value);

            rbtree_remove(&fData->node,&tree);
            free (fData->key);  // Free the DedupData
            free (fData);
            numElements--;
        }
    }

}

void write_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    for (int i=0; i < cnt/BLOCKSIZE; i++)
    {
        unsigned long int tempoffset = offset+i*BLOCKSIZE;
        remove_block (tempoffset);
    }
}

void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    // Nothing is done in the read phase
    // We can split the dedupl function into check cache, fill cache but it is clear in a single function

}

typedef struct argsTree {
	unsigned char * buffer;
	unsigned long int offset;
} argsTree;





void insertToTree(unsigned char * buffer, unsigned long int offset)
{
//struct argsTree * args = (struct argsTree*) arg;
// Move all the tree insertion operation to a thread // Beware of locks
unsigned char * new_buf = buffer;
unsigned long int tempoffset = offset;

struct DedupData dd;
dd.key = new_buf;
struct rbtree_node * data2 = rbtree_lookup (&dd.node, &tree);

if (data2)
{
	 // It's in the cache
                // Insert on the second tree : Offset : tree pointer
    struct DedupData * fData = rbtree_container_of (data2, struct DedupData, node);
    struct OffsetData *off = malloc (sizeof(struct OffsetData));
    off->key  = tempoffset;

    off->realdata = fData;
   // pthread_mutex_lock (&tree_mutex);
    struct rbtree_node * dtemp = rbtree_insert (&off->node, &offsettree);
   // pthread_mutex_unlock (&tree_mutex);
    // We increase the number of references...

    fData->counter++;
    
    InsertAtHead(&fData->head,off);  // We store nodes directly

    free (new_buf); // We can remove the readed data
}
else
{

 // It's a new page
				
                // Check if we hit maximum number
                if (numElements >= MAXSIZE)
                {
                    // remove less referenced element
                    // We need some structures:
                    //	priority queue [ Pointer ] [ Reference Counter ] [ Offset Pointer]
                    // But for now do at random

                	//  Implementation 1 : Random eviction
                  /*  unsigned long long randomvalue = rand()%(numElements-1);
                    struct rbtree_node * nodeR = rbtree_first (&tree);
                    for (unsigned long long e = 0 ; e < randomvalue; e++)
                        nodeR = rbtree_next(nodeR);
*/


                    // Implementation 2 : Less deduplicated
                  //  pthread_mutex_lock (&tree_mutex);
                    struct rbtree_node * nodeR = rbtree_first (&tree);
                    struct rbtree_node * minimumNode = nodeR;
                    struct DedupData * fData = rbtree_container_of (nodeR, struct DedupData, node);
                    unsigned int  minimum = fData->counter;
                    while (nodeR && minimum > 10)
                    {
                    	fData = rbtree_container_of (nodeR, struct DedupData, node);
                    	if ( minimum > fData->counter) {
                    		minimum = fData->counter;
                    		minimumNode = nodeR;
                    	}
                    	nodeR = rbtree_next(nodeR);
                    }

/*			// Implementation 3 : Last element in the tree
			struct rbtree_node * nodeR = rbtree_last(&tree);*/
                    struct DedupData *fData2 = rbtree_container_of(nodeR, struct DedupData, node);

                    RemoveAll (&fData2->head);
                    rbtree_remove(&fData2->node, &tree);
                    free (fData2->key);
//		    free (fData2->pointer);
                    free (fData2);
                    numElements--;
                 //   pthread_mutex_unlock(&tree_mutex);

                }

                struct DedupData * dedupdata = malloc (sizeof(struct DedupData));
//                dedupdata->key = str2md5(new_buf,BLOCKSIZE);
//		dedupdata->pointer = new_buf;
                dedupdata->counter = 1;
		dedupdata->key = new_buf;
                dedupdata->head = NULL;
                pthread_mutex_lock(&tree_mutex);
                struct rbtree_node * newCache = rbtree_insert(&dedupdata->node, &tree);
                pthread_mutex_unlock(&tree_mutex);
                struct OffsetData * offsetdata = malloc (sizeof(struct OffsetData));
                offsetdata->key = tempoffset;
                offsetdata->realdata = dedupdata;
                pthread_mutex_lock(&tree_mutex);
                struct rbtree_node * dtemp = rbtree_insert (&offsetdata->node,&offsettree);
                pthread_mutex_unlock(&tree_mutex);
                InsertAtHead  (&dedupdata->head, offsetdata);
                numElements++;

}


return NULL;

}

void *processInsert (void *arg)
{
	unsigned char * pending_buf;
	unsigned long int pending_off;
	
	while (1)
	{
		pthread_mutex_lock (&process_mutex);
		while (pending_list == NULL) {
			pthread_cond_wait(&process_cond, &process_mutex); 
		}
		//int pending = 0;
		//while (pending_list != NULL)
		//{
			//pthread_cond_wait(&process_cond, &process_mutex);
			if ( pending_list != NULL)
			{
				pending_buf = ((struct argsTree*)(pending_list->data))->buffer;
				pending_off = ((struct argsTree*)(pending_list->data))->offset;
				free (pending_list->data);
				RemoveHead(&pending_list);
				pthread_mutex_unlock(&process_mutex);
				insertToTree (pending_buf, pending_off);  // Tree is not thread safe 
				
		}
			else pthread_mutex_unlock(&process_mutex);
	}
	return NULL; 
}

int pre_read(void* buf, unsigned long cnt, unsigned long int offset, unsigned int fd, bool *doRead)
{
    int cached = 0;
    char line[200];

    if (init == 0) {
        rbtree_init(&tree,KeyComp, 0);
        rbtree_init(&offsettree,OffsetComp,0);
	//pthread_create(&thread1, NULL, processInsert, NULL);
	
        init = 1;
    }

    if(cnt < BLOCKSIZE) return 0;

    *doRead = false;


    for (int i=0; i < cnt/BLOCKSIZE; i++)
    {
        unsigned char* my_buf = (unsigned char *) (buf+i*BLOCKSIZE);
        unsigned long int tempoffset = offset+i*BLOCKSIZE;

        struct OffsetData off;
        off.key =  tempoffset;

        struct rbtree_node *data = rbtree_lookup (&off.node,&offsettree);


        if ( data )
        {
            struct OffsetData * fOff = rbtree_container_of (data, struct OffsetData, node);
            struct DedupData * fData = fOff->realdata;

            memcpy (my_buf, fData->key, BLOCKSIZE);
            cached++;
        }
        else
        {
            unsigned char * new_buf = malloc (BLOCKSIZE);
            lseek(fd, offset+i*BLOCKSIZE, SEEK_SET);
            read (fd, new_buf, BLOCKSIZE);

            memcpy (my_buf, new_buf, BLOCKSIZE);

         	insertToTree (new_buf, tempoffset);  // Tree is not thread safe 
/*
            struct argsTree *args = malloc (sizeof(struct argsTree));
            args->buffer = new_buf;
            args->offset = tempoffset;
	   
            pthread_mutex_lock(&process_mutex);
            InsertAtTail(&pending_list, args);
            pthread_cond_signal (&process_cond);
            pthread_mutex_unlock (&process_mutex);
          */
           

	    //sleep (1);
       		//insertToTree (new_buf, &tempoffset);
        }


    }
// Leftovers
// */
//

    if ((cnt-((cnt/BLOCKSIZE)*BLOCKSIZE))>0)
    {
        lseek(fd, offset+((cnt/BLOCKSIZE)*BLOCKSIZE), SEEK_SET);
        read (fd, buf+((cnt/BLOCKSIZE)*BLOCKSIZE),cnt-((cnt/BLOCKSIZE)*BLOCKSIZE));
    }
    return cached;
}

int get_element(void * buf, unsigned long cnt, unsigned long int offset)
{
    return 0;

}
