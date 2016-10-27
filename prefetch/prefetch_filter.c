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
#include "prefetch_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>


int g_flag = 0;
char *filter_name = "PREFETCH";
char *filename = "/tmp/prefetchlist.txt";  // Filename where it stores the offset - size list


char *get_name()
{
    return filter_name;
}

void print_bind_msg(char *msg)
{
    printf("%s Prefetch Filter\n", msg);
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


void store_info (char * line)
{
    int fd = open (filename, O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}



void write_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    //unsigned long i;
    //unsigned char* my_buf = (unsigned char*)buf;
}

void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    //unsigned long i;
    //unsigned char* my_buf = (unsigned char*)buf;


    //struct timeval tv;
    char str[100];
    //  gettimeofday(&tv, NULL);


    sprintf(str,"%lu %lu\n", offset, cnt); // Change to binary format, move to a thread that writes async the information
    store_info (str);

}


int pre_read(void* buf, unsigned long cnt, unsigned long int offset,unsigned int fdesc, bool * doRead)
{   *doRead = true;
    return 0;
}