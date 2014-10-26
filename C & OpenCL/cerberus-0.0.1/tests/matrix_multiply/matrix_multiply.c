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
#include <time.h>
#include <inttypes.h>
#include <sys/time.h>

#include "map_reduce.h"
#include "stddefines.h"


typedef struct
{
	cl_uint v1;
	cl_uint v2;
} input_t;

typedef struct
{
	cl_uint key;
	cl_int value;
} keyval_t;

static int *genMatrix(int matrix_len)
{
	int *matrix = (int*)malloc(sizeof(int)* matrix_len * matrix_len * 2);

	for (int i = 0; i < matrix_len; i++)
		for (int j = 0; j < matrix_len; j++)
			matrix[i*matrix_len+j] = (rand())%11;
			
	for (int i = matrix_len; i < matrix_len*2; i++)
		for (int j = 0; j < matrix_len; j++)
			matrix[i*matrix_len+j] = (rand())%11;			

	return matrix;
}

void mm_merger(merger_dat_t* data)
{
	//keyval_t* keyvals = (keyval_t*)data->keyvals;
	//size_t length = data->size;
	
	data->output = data->keyvals;
	data->output_size = data->size;
}

int main(int argc, char *argv[]) {

    int i,j;
    int matrix_len;
	int* fdata;

    struct timeval begin, end;

    get_time (&begin);
    
    srand( (unsigned)time( NULL ) );

	size_t num_workitems = 0;
	size_t num_workgroups = 0;
	
    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <matrix_len> <num workgroups> <workgroup size>\n", argv[0]);
        exit(1);
    }
    	
		// Obtain custom work size
	if (argv[2] != NULL && argv[3] != NULL)
	{
		num_workitems = atoi(argv[2]);
		num_workgroups = atoi(argv[3]);
	}


    CHECK_ERROR ( (matrix_len = atoi(argv[1])) < 0);

    if(argv[1] == NULL)
        matrix_len = 128;
    else
        CHECK_ERROR ( (matrix_len = atoi(argv[1])) < 0);

	fdata = genMatrix(matrix_len);

    printf("MatrixMult: Side of the matrix is %d\n", matrix_len);
    printf("MatrixMult: Running...\n");

	input_t* data = (input_t*)malloc(sizeof(input_t) * matrix_len * matrix_len);
	for(i = 0; i < matrix_len; i++)
	{
		for(j = 0; j < matrix_len; j++)
		{
			data[i*matrix_len + j].v1 = i;
			data[i*matrix_len + j].v2 = j;
		}
	}

    CHECK_ERROR (fdata == NULL);
    CHECK_ERROR (map_reduce_init ());

	size_t res_len;
	
    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = data;
	strcpy(map_reduce_args.map, "mm_map.cl");
	sprintf(map_reduce_args.map_args, "-D ROW_NUM=%d", matrix_len);
	map_reduce_args.merger = &mm_merger;
	map_reduce_args.num_output_per_map_task = 1;
	if (num_workgroups > 0)
		map_reduce_args.num_workgroups = num_workgroups;
	else
		map_reduce_args.num_workgroups = 7;
		
	if (num_workitems > 0)
		map_reduce_args.num_workitems = num_workitems;
	else
		map_reduce_args.num_workitems = 0;
					
	// We put the matrix as aux data as its constant between workgroups
	map_reduce_args.map_aux_arg = fdata;
	map_reduce_args.map_aux_size = matrix_len * matrix_len * 2 * sizeof(int);
	
    map_reduce_args.unit_size = sizeof(input_t);
    map_reduce_args.keyval_size = sizeof(keyval_t);
    map_reduce_args.partition = NULL; 
    map_reduce_args.result_len = &res_len;
    map_reduce_args.data_size = sizeof(input_t) * matrix_len * matrix_len;

    fprintf(stderr, "***** data size is %" PRIdPTR "\n", (intptr_t)map_reduce_args.data_size);
    printf("MatrixMult: Calling MapReduce Scheduler Matrix Multiplication\n");

    get_time (&end);

    fprintf (stderr, "initialize: %ld\n", time_diff (&end, &begin));

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

    fprintf (stderr, "library: %ld\n", time_diff (&end, &begin));

    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());

    dprintf("MatrixMult: MapReduce Completed\n");

    get_time (&end);

    fprintf (stderr, "finalize: %ld\n", time_diff (&end, &begin));

    return 0;
}
