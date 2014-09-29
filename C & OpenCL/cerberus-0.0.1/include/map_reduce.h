/*  Karol Pogonowski - Master of Informatics Dissertation
	This is an OpenCL MapReduce library written in C/OpenCL and primarily
	targeting GPU computing.
*/

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
*     * Neither the name of Stanford University nor the
*       names of its contributors may be used to endorse or promote products
*       derived from this software without specific prior written permission.
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

#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include "stdbool.h"
#include "stddef.h"
#include "CL/cl.h"
#include <sys/time.h>

#define MAX_FILENAME 128

// Data structure for holding splitter data to be fed to map tasks
typedef struct
{
	void* pointer;
	size_t length;
} splitter_array_t;

// Data structure for merger input and output
typedef struct
{
	void* keyvals;
	size_t size;
	void* output;
	size_t output_size;
} merger_dat_t;
  
typedef void (*splitter_t)(void *);
typedef void (*partition_t)(void *);
typedef void (*merger_t)(merger_dat_t*);


/* The arguments to operate the runtime. */
typedef struct
{
    void * task_data;           /* The data to run MapReduce on.
                                 * If splitter is NULL, this should be an array. */
    size_t data_size;            /* Total # of bytes of data */

	// Those arguments must match ones used in kernels
	size_t keyval_size;

	size_t unit_size;		/* Input unit size */
	size_t num_output_per_map_task; // Number of map outputs per input unit
	
	char map[MAX_FILENAME];				/* Name of the map kernel */
	char map_count[MAX_FILENAME];				/* Name of the map count kernel */
	char map_args[256];			/* Additional defines for map kernel */   
   
	char  reduce[MAX_FILENAME];            /* Name of the reduce kernel */     
	char  reduce_count[MAX_FILENAME];     /* Name of the reduce count kernel */     
	char reduce_args[256]; 	/* Additional defines for reduce kernel */   
	
	
	char  sort[MAX_FILENAME];            /* Name of the sort kernel */  

	splitter_t splitter;        /* If NULL, the array splitter is used.*/

    partition_t partition;      /* Default partition function is a 
                                 * hash function */	
								 
	merger_t merger;

	void* map_aux_arg;
	size_t map_aux_size;
	
	size_t num_workgroups;
	size_t num_workitems;
	size_t tasks_per_reduce;


    void *result;       /* Pointer to output data. 
                                 * It is allocated in the merger function*/
    size_t *result_len; /* Length of resulting data */
	
} map_reduce_args_t;

/* Runtime defined functions. */

/* MapReduce initialization function. Called once per process. */
int map_reduce_init ();

/* MapReduce finalization function. Called once per process. */
int map_reduce_finalize ();

/* The main MapReduce engine. This is the function called by the application.
 * It is responsible for creating and scheduling all map and reduce tasks, and
 * also organizes and maintains the data which is passed from application to 
 * map tasks, map tasks to reduce tasks, and reduce tasks back to the
 * application. Results are stored in args->result. A return value less than zero
 * represents an error. This function is not thread safe. 
 */   
int map_reduce (map_reduce_args_t * args);


/* Internal map reduce state. */
typedef struct
{
    /* Parameters. */

	bool buildLogging;			/* Enables logging of kernel build process */

	/* Callbacks. */
	// Should be CPU
	cl_kernel map;                      /* Map function. */
	cl_kernel map_count;                      /* Map function. */
	//partition_t partition;          /* Partition function. */     
	// Sorted data speeds up reduce phase since:
	// 1) Less local memory is used to track count
	// 2) Shorter kernels
	// 3) Easier merge && data locality
	cl_kernel reduce;                /* Reduce function. */
	cl_kernel reduce_count;                /* Reduce function. */

	// This must be defined for sort and other stuff
   // key_cmp_t key_cmp;              /* Key comparator function. */

    /* Structures. */
    map_reduce_args_t * args;       /* Args passed in by the user. */

	cl_mem* input_array; /* Array to send to map task. */
	cl_mem* map_array; /* Map output */

	cl_uint* map_array_size;
	cl_uint* reduce_array_size;

	/* Array to send to merge task. */
	cl_mem* reduce_array;
	cl_mem* merged_map_array;

	// Methods for output write synchronization
	cl_uint* map_data_size;
	cl_uint* reduce_data_size;
	
	void* map_aux_arg;
	size_t map_aux_size;

	// OpenCL specific
	cl_context device_context;
	cl_command_queue device_queue;
	cl_device_id device;
	cl_program map_program;
	cl_program map_count_program;
	cl_program reduce_program;
	cl_program reduce_count_program;

	size_t num_workitems;
	size_t num_workgroups;
	size_t num_reduce_workgroups;
	size_t num_reduce_workitems;
	cl_uint	num_compute_units;
	size_t max_workitems;
	
	splitter_array_t *splitter_data;

} mr_env_t;


void default_splitter (void*);
void default_partition (void*);

#endif // MAP_REDUCE_H_
