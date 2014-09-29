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

typedef struct
{
    float key;
	cl_int2 value;
} keyval_t;

void ss_merger(merger_dat_t* data)
{	
	data->output = data->keyvals;
	data->output_size = data->size;
}
	
static float *genMatrix(int num_docs, int vector_size)
{
	float *matrix = (float*)malloc(sizeof(float)*num_docs*vector_size);

	for (int i = 0; i < num_docs; i++)
		for (int j = 0; j < vector_size; j++)
			matrix[i*vector_size+j] = (float)(rand() % 100);

	return matrix;
}

int main(int argc, char *argv[]) 
{
    struct timeval begin, end;
	size_t num_workitems = 0;
	size_t num_workgroups = 0;

    get_time (&begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <num docs> <vector size> <num workitems> <num workgroups>\n", argv[0]);
        exit(1);
    }
    
	int num_docs = atoi(argv[1]);
	int vector_size = atoi(argv[2]);
	
	// Obtain custom work size
	if (argv[3] != NULL && argv[4] != NULL)
    {
		num_workitems = atoi(argv[3]);
		num_workgroups = atoi(argv[4]);
	}

    printf("Similarity Score: Running...\n");
	

	float *matrix = genMatrix(num_docs, vector_size);
    
	cl_int2* doc_info = malloc(sizeof(cl_int2) * num_docs * num_docs);
	for (int i = 0; i < num_docs; i++)
	{
		for (int j = 0; j < num_docs; j++)
		{
			doc_info[i*num_docs + j].s[0] = i;
			doc_info[i*num_docs + j].s[1] = j;
		}
	}
	
    CHECK_ERROR (map_reduce_init ());
	
	size_t res_len;

    // Setup scheduler args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = doc_info; 
	strcpy(map_reduce_args.map, "ss_map.cl");
	sprintf(map_reduce_args.map_args, "-D VEC_SIZE=%d", vector_size);
	map_reduce_args.merger = &ss_merger;
	map_reduce_args.num_output_per_map_task = 1;
	if (num_workgroups > 0)
		map_reduce_args.num_workgroups = num_workgroups;
	else
		map_reduce_args.num_workgroups = 128;
		
	if (num_workitems > 0)
		map_reduce_args.num_workitems = num_workitems;
	else
		map_reduce_args.num_workitems = 0;
		
	map_reduce_args.map_aux_arg = matrix;
	map_reduce_args.map_aux_size = sizeof(float) * num_docs * vector_size;

    map_reduce_args.splitter = NULL;
    map_reduce_args.unit_size = sizeof(cl_int2);
    map_reduce_args.keyval_size = sizeof(keyval_t);
    map_reduce_args.partition = NULL; 
    map_reduce_args.result_len = &res_len;
    map_reduce_args.data_size = sizeof(cl_int2) * num_docs * num_docs;
	
    printf("SS: Calling MapReduce Scheduler\n");

    get_time (&end);

    fprintf (stderr, "initialize: %ld\n", time_diff (&end, &begin));

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

    fprintf (stderr, "library: %ld\n", time_diff (&end, &begin));
	
    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());
	
    get_time (&end);

    fprintf (stderr, "finalize: %ld\n", time_diff (&end, &begin));

    return 0;
}
