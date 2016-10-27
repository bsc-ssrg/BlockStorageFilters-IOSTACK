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
#include "prefetch2_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "libtree.h"

int g_flag = 0;
char *filter_name = "PREFETCH2";
char *filename = "/tmp/prefetchlist.txt";

int count = 0;
unsigned long long number_elements;
#define NUM_PREFETCH 10000
// 10000 Cached reads

FILE *fd_p = NULL;
bool init = false;
bool end = false;

struct PrefetchData
{
    char * key;  // ·OFFSET SIZE·
    void * content; // Number of references
    struct rbtree_node node;
};


int  KeyComp(const struct rbtree_node * a, const struct rbtree_node * b) {
    struct PrefetchData *p = rbtree_container_of(a, struct PrefetchData, node);
    struct PrefetchData *q = rbtree_container_of(b, struct PrefetchData, node);
    int comp = strcmp (p->key,q->key);
    if (comp > 0) return 1;
    if (comp < 0) return -1;
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



void store_debug (char * line)
{
    int fd = open ("/tmp/debugprefetch", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}


void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
}

bool read_prefetch (unsigned long *cnt, unsigned long int *offset)
{
    char *buffer = NULL; // Not safe, just for demo
    size_t bufsize=0;
    ssize_t characters;

    if ((characters = getline(&buffer,&bufsize,fd_p)) != -1)
    {
        char *pch, *endptr;

        pch = strtok (buffer," ");

        while (pch != NULL)
        {
            unsigned long long value = strtoll (pch, &endptr,10);
            *offset = value;
            pch = strtok (NULL," ");
            value = strtoll (pch,&endptr, 10);
            *cnt = value;
            pch = strtok (NULL," ");
        }

    }
    else
    {
        fclose (fd_p);
        return true;
    }
    free (buffer);
    return false;

}


bool prefetch_one (unsigned int fd)
{

    unsigned long new_cnt;
    unsigned long int new_offset;

    end = read_prefetch (&new_cnt, &new_offset);
    if (end) return false;
    struct PrefetchData * prefetchdata = malloc (sizeof(struct PrefetchData));
    prefetchdata->key = malloc (100);
    sprintf (prefetchdata->key,"%lu %lu",new_offset, new_cnt);
    prefetchdata->content = malloc (new_cnt);
    lseek (fd,new_offset, SEEK_SET);
    read (fd,prefetchdata->content,new_cnt);
    rbtree_insert(&prefetchdata->node, &tree);
    return true;
}


bool recover_one (void *buf, unsigned long cnt, unsigned long int offset)
{
    struct PrefetchData checkprefetch;
    checkprefetch.key = malloc(100);
    sprintf (checkprefetch.key,"%lu %lu",offset, cnt);


    struct rbtree_node * node_p = rbtree_lookup (&checkprefetch.node, &tree);
    if (node_p)
    {

        struct PrefetchData * pData = rbtree_container_of (node_p, struct PrefetchData, node);
        memcpy (buf, pData->content,cnt);

        rbtree_remove(&pData->node,&tree);
        free(pData->content);
        free(pData->key);
        number_elements--;
        free(checkprefetch.key);
        return false;
    }
    else
    {
        free (checkprefetch.key);
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
        init = true;
    }


    if ( !end && number_elements == NUM_PREFETCH)
    {
        struct rbtree_node * node_p = rbtree_first (&tree);
        struct PrefetchData * pData = rbtree_container_of (node_p, struct PrefetchData, node);
        rbtree_remove(&pData->node,&tree);
        free(pData->content);
        free(pData->key);
        number_elements--;
    }
    while (!end && number_elements < NUM_PREFETCH)
    {
        end = prefetch_one(fd);
        if ( !end )
            number_elements++;
    }

    // Look for block asked
    {
        *doRead = recover_one (buf, cnt, offset);
    }
    return 0;
}
void write_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    char * mybuf = malloc(cnt);
    recover_one(mybuf,cnt,offset);
    free (mybuf);
}


