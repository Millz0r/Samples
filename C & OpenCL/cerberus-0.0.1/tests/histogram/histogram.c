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
#define IMG_DATA_OFFSET_POS 10
#define BITS_PER_PIXEL_POS 28
#define TIMING 

typedef struct
{
	cl_int key;
	cl_uint value;
} keyval_t;

void histogram_merger(merger_dat_t* data)
{
	keyval_t* keyvals = (keyval_t*)data->keyvals;
	size_t length = data->size;
	
	unsigned int* out = malloc(sizeof(unsigned int) * 768);
	
	for(size_t i = 0; i < 768; i++)
		out[i] = 0;
	
	for(size_t i = 0; i < length; i++)
	{
		int idx = keyvals[i].key;
		unsigned int curr = keyvals[i].value;
		if (idx < 768)
			out[idx] += curr;
	}

	// Remember to update the data length
	data->output_size = 768;
	data->output = out;
}

int main(int argc, char *argv[]) 
{
    int fd;
    char *fdata;
    struct stat finfo;
    char * fname;
    struct timeval begin, end;
	size_t num_workitems = 0;
	size_t num_workgroups = 0;

    get_time (&begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <bitmap filename> <num workitems> <num workgroups>\n", argv[0]);
        exit(1);
    }
    
    fname = argv[1];
	// Obtain custom work size
	if (argv[2] != NULL && argv[3] != NULL)
    {
		num_workitems = atoi(argv[2]);
		num_workgroups = atoi(argv[3]);
	}

    printf("Histogram: Running...\n");
    
    // Read in the file
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
    // Memory map the file
    CHECK_ERROR((fdata = mmap(0, finfo.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
#else
    int ret;
        
    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

    if ((fdata[0] != 'B') || (fdata[1] != 'M')) {
        printf("File is not a valid bitmap file. Exiting\n");
        exit(1);
    }
        
    unsigned short *bitsperpixel = (unsigned short *)(&(fdata[BITS_PER_PIXEL_POS]));
    if (*bitsperpixel != 24) {     // ensure its 3 bytes per pixel
        printf("Error: Invalid bitmap format - ");
        printf("This application only accepts 24-bit pictures. Exiting\n");
        exit(1);
    }
    
    unsigned short *data_pos = (unsigned short *)(&(fdata[IMG_DATA_OFFSET_POS]));
    
    int imgdata_bytes = (int)finfo.st_size - (int)(*(data_pos));
    printf("This file has %d bytes of image data, %d pixels\n", imgdata_bytes,
                                                                                imgdata_bytes / 3);

    CHECK_ERROR (map_reduce_init ());

    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &(fdata[*data_pos]); 
	strcpy(map_reduce_args.map, "hist_map.cl");
	strcpy(map_reduce_args.reduce, "hist_reduce.cl");
	strcpy(map_reduce_args.reduce_count, "hist_reduce_count.cl");
	map_reduce_args.merger = &histogram_merger;
    map_reduce_args.tasks_per_reduce = 2;
	map_reduce_args.num_output_per_map_task = 3;
	if (num_workgroups > 0)
		map_reduce_args.num_workgroups = num_workgroups;
	else
		map_reduce_args.num_workgroups = 7;
		
	if (num_workitems > 0)
		map_reduce_args.num_workitems = num_workitems;
	else
		map_reduce_args.num_workitems = 0;
		
    map_reduce_args.unit_size = (sizeof(cl_uint) * 3);  // 3 bytes per pixel
    map_reduce_args.data_size = imgdata_bytes;
    map_reduce_args.keyval_size = sizeof(keyval_t);
	map_reduce_args.partition = NULL; 

    fprintf(stderr, "Histogram: Calling MapReduce OpenCL Runtime\n");

    get_time (&end);

    fprintf (stderr, "initialize: %ld\n", time_diff (&end, &begin));

    get_time (&begin);
    CHECK_ERROR( map_reduce (&map_reduce_args) < 0);
    get_time (&end);

    fprintf (stderr, "library: %ld\n", time_diff (&end, &begin));

    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());

#ifndef NO_MMAP
    CHECK_ERROR (munmap (fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR (close (fd) < 0);

    get_time (&end);

    fprintf (stderr, "finalize: %ld\n", time_diff (&end, &begin));

    return 0;
}
