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
#include <sys/time.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <sys/times.h>
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"

#define WORD_LENGTH 16
#define LINE_LENGTH 128

typedef struct
{
	cl_char x[LINE_LENGTH];
} input_t;

typedef struct
{
	cl_char x[WORD_LENGTH];
} word_t;

typedef struct
{
	cl_int key;
	cl_int value;
} keyval_t;

int is_letter(const cl_char curr_ltr)
{
	if ((curr_ltr >= 'A' && curr_ltr <= 'Z') || (curr_ltr >= 'a' && curr_ltr <= 'z'))
		return 1;
	return 0;
}

void sm_splitter (void* input)
{
	mr_env_t *env = (mr_env_t*)input;
	
	// Partition input into num_workgroups memory buffers
	env->splitter_data = (splitter_array_t*)malloc(sizeof(splitter_array_t) * env->num_workgroups);
	unsigned int curr_chunk = 0;
	unsigned int num_data = env->args->data_size / sizeof(cl_char);
	cl_char* t_data = (cl_char*)env->args->task_data;
	
	// First calculate the number of tasks
	unsigned int num_tasks = 0;
	int i = 0;
	while (i < num_data)
	{
		// Look for non-letters at the end of max line length
		int idx = 0;
		
		for(idx = LINE_LENGTH - 1; idx > 0; idx--)
		{
			if (i + idx >= num_data)
				continue;

			if (is_letter(t_data[i + idx]) == 0)
			{
				num_tasks++;
				break;
			}
		}	
		
		i += idx + 1;
	}
		
	unsigned int tasks_per_map = div_round_up(num_tasks, (env->num_workgroups * env->num_workitems));
    fprintf (stderr, "splitter num tasks: %u\n", num_tasks);
	for(i = 0; i < env->num_workgroups; i++)
	{
		input_t* curr_ptr = malloc(sizeof(input_t) * tasks_per_map * env->num_workitems);
		for(int j = 0; j < tasks_per_map * env->num_workitems; j++)
		{				
			// Look for non-letter
			int idx = 0;
			for(idx = LINE_LENGTH - 1; idx > 0; idx--)
			{
				if (curr_chunk + idx >= num_data)
					continue;
					
				if (is_letter(t_data[curr_chunk + idx]) == 0)
					break;
			}
			
			if (idx == 0)
			{
				// End of input data, set to NULL
				curr_ptr[j].x[0] = '\0';				
			}
			else
			{
				memcpy(curr_ptr[j].x, &t_data[curr_chunk], idx+1);
				// Add null
				if(idx < LINE_LENGTH - 2)
					curr_ptr[j].x[idx+1] = '\0';
				curr_chunk += idx+1;				
			}
		}
		
		env->splitter_data[i].pointer = curr_ptr;
		env->splitter_data[i].length = (sizeof(input_t) * tasks_per_map * env->num_workitems);
	}
}

void sm_merger(merger_dat_t* data)
{
	keyval_t* keyvals = (keyval_t*)data->keyvals;
	size_t length = data->size;
	cl_uint* counts = malloc(sizeof(cl_uint) * 4);
	
	for(size_t i = 0; i < 4; i++)
		counts[i] = 0;
		
	for(size_t i = 0; i < length; i++)
	{
		counts[keyvals[i].key]++;
	}
	
	data->output = counts;
	data->output_size = 4;
}

int main(int argc, char *argv[]) 
{
    int fd;
    char *fdata;
    struct stat finfo;
    char *fname;

    struct timeval begin, end;

    get_time (&begin);

	size_t num_workitems = 0;
	size_t num_workgroups = 0;

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename> <num workgroups> <workgroup size>\n", argv[0]);
        exit(1);
    }
    	
	// Obtain custom work size
	if (argv[2] != NULL && argv[3] != NULL)
	{
		num_workitems = atoi(argv[2]);
		num_workgroups = atoi(argv[3]);
	}

    fname = argv[1];

    printf("String Match: Running...\n");

    // Read in the file
    CHECK_ERROR((fd = open(fname,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
    // Memory map the file
    CHECK_ERROR((fdata= mmap(0, finfo.st_size + 1,
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
#else
    int ret;

    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

    CHECK_ERROR (map_reduce_init ());
	
	// Size of result
	size_t res_len;

    // Setup scheduler args	
	map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = fdata;
	strcpy(map_reduce_args.map, "sm_map.cl");
	strcpy(map_reduce_args.map_count, "sm_map_count.cl");
	map_reduce_args.merger = &sm_merger;
    map_reduce_args.splitter = &sm_splitter;
	if (num_workgroups > 0)
		map_reduce_args.num_workgroups = num_workgroups;
	else
		map_reduce_args.num_workgroups = 0;
		
	if (num_workitems > 0)
		map_reduce_args.num_workitems = num_workitems;
	else
		map_reduce_args.num_workitems = 0;
		
    map_reduce_args.unit_size = sizeof(input_t);
    map_reduce_args.keyval_size = sizeof(keyval_t);
    map_reduce_args.partition = NULL; 
    map_reduce_args.result_len = &res_len;
    map_reduce_args.data_size = finfo.st_size;

    printf("String Match: Calling String Match\n");

    get_time (&end);

    fprintf (stderr, "initialize: %ld\n", time_diff (&end, &begin));

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

    fprintf (stderr, "library: %ld\n", time_diff (&end, &begin));

    printf("String Match: Completed %ld\n", time_diff (&end, &begin));
	
    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());
	

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR(close(fd) < 0);

    get_time (&end);

    fprintf (stderr, "finalize: %ld\n", time_diff (&end, &begin));

    return 0;
}
