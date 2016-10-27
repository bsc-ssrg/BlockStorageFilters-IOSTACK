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
#include "mockup_filter.h"
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h> 


int g_flag = 0;
char *filter_name = "MOCKUP";

#define ITERATIONS 20        // Number of iterations per block read or written
#define LATENCY 100          // latency in us, to include on each read or written block.

void store_debug (char * line)
{
    int fd = open ("/tmp/debugmockup", O_WRONLY|O_APPEND|O_CREAT);  // Not the best performant
    if (fd < 0) print_bind_msg ("Failed message");
    int bytes = write (fd, line, strlen(line));
    if (bytes < 0 ) print_bind_msg ("Lower write");
    close(fd);
}

unsigned int id = 0;
char *get_name()
{
    return filter_name;
}



void print_bind_msg(char *msg)
{
    printf("%s Output Mockup \n", msg);
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



unsigned long long GetTimeStamp() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec*(unsigned long long)1000000+tv.tv_usec;
}

/* TODO: Spent_time can be written to the log file in order to see the variations and interferences produced by other filters 
The value should be nicely stable, if the CPU is not overloaded.
*/

void cpuUsageGenerator(unsigned int iterations, void * buf, unsigned long cnt )
{

    if ( id == 0) 
        { 
            srand(time(NULL));
            id = rand();
        }

    char * buffer = (char * )buf;
    unsigned long long initial_time = GetTimeStamp();
    // Please, remove O3... optimizations in general
    unsigned long long value = 1;
    for (int i = 0; i < iterations; i++)
    {   
       for (int j = 0 ; j < cnt; j++)
       {
        value *= buffer[j];
       }
    }

    unsigned long long spent_time = GetTimeStamp() - initial_time;
}


void latencyGenerator(unsigned long long usecs)
{
    usleep (usecs);         // It does not depend of the load of the system    
}



// We do the reads on the prefetch so in the read we do not do anything

void read_xform( void* buf, unsigned long cnt, unsigned long int offset)
{
    cpuUsageGenerator (ITERATIONS, buf, cnt);
    latencyGenerator (LATENCY);
}




/* Prefetch a number of blocks, and give one of them if available*/
int pre_read(void* buf, unsigned long cnt, unsigned long int offset, unsigned int fd, bool *doRead)
{

   *doRead = true;

    return 0;
}


//
void write_xform( void* buf, unsigned long cnt, unsigned long int offset, bool *doWrite)
{
    cpuUsageGenerator (ITERATIONS, buf, cnt);
    latencyGenerator (LATENCY);
    *doWrite = true;
}
