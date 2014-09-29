/* Copyright (c) 2007-2009, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/time.h>
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"


typedef struct {
    char x;
    char y;
} POINT_T;

enum {
    KEY_SX = 0,
    KEY_SY,
    KEY_SXX,
    KEY_SYY,
    KEY_SXY
};

typedef struct
{
	cl_int key;
	cl_long value;
} keyval_t;

void linear_regression_merger(merger_dat_t* data)
{
	keyval_t* keyvals = (keyval_t*)data->keyvals;
	size_t length = data->size;
	cl_long* sum = (cl_long*)malloc(sizeof(cl_long) * 5);
	
	for(size_t i = 0; i < 5; i++)
		sum[i] = 0;	
	
	for(size_t i = 0; i < length; i++)
	{
		cl_int curr_key = keyvals[i].key;
		cl_long curr_val = keyvals[i].value;
		
		sum[curr_key] += curr_val;
	}

	free(keyvals);
	
	data->output = sum;
	data->output_size = 5;
}
	

int main(int argc, char *argv[]) {

    int fd;
    char * fdata;
    char * fname;
    struct stat finfo;
    int i;
	bool create_files = false;

    struct timeval starttime,endtime;
    struct timeval begin, end;
	size_t num_workitems = 0;
	size_t num_workgroups = 0;

    get_time (&begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename> <num workgroups> <workgroup size>\n", argv[0]);
        exit(1);
    }
    
    fname = argv[1];
	
	// Obtain custom work size
	if (argv[2] != NULL && argv[3] != NULL)
    {
		num_workitems = atoi(argv[2]);
		num_workgroups = atoi(argv[3]);
	}

    printf("Linear Regression: Running...\n");
	
    
    // Read in the file
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
    // Memory map the file
    CHECK_ERROR((fdata = mmap(0, finfo.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
#else
    uint64_t ret;

    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

	
    CHECK_ERROR (map_reduce_init ());
	
	size_t res_len;

    // Setup scheduler args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = fdata; // Array to regress
	strcpy(map_reduce_args.map, "linear_map.cl");
	strcpy(map_reduce_args.reduce, "linear_reduce.cl");
	strcpy(map_reduce_args.reduce_count, "linear_reduce_count.cl");
	map_reduce_args.merger = &linear_regression_merger;
	map_reduce_args.num_output_per_map_task = 5;
	map_reduce_args.tasks_per_reduce = 8;
	if (num_workgroups > 0)
		map_reduce_args.num_workgroups = num_workgroups;
	else
		map_reduce_args.num_workgroups = 7;
		
	if (num_workitems > 0)
		map_reduce_args.num_workitems = num_workitems;
	else
		map_reduce_args.num_workitems = 0;
		
    map_reduce_args.splitter = NULL; 
    map_reduce_args.unit_size = sizeof(cl_int2);
    map_reduce_args.keyval_size = sizeof(keyval_t);
    map_reduce_args.partition = NULL; 
    map_reduce_args.result_len = &res_len;
    map_reduce_args.data_size = finfo.st_size - (finfo.st_size % map_reduce_args.unit_size);
	
    printf("Linear Regression: Calling MapReduce Scheduler\n");

    gettimeofday(&starttime,0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());

    gettimeofday(&endtime,0);

    printf("Linear Regression: Completed %u ms\n",time_diff (&endtime, &starttime));

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR(close(fd) < 0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
