/*  Karol Pogonowski - Master of Informatics Dissertation
    This is an OpenCL MapReduce library written in C/OpenCL and primarily
    targeting GPU computing.
*/

/* Copyright(c) 2007-2009, Stanford University
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
*(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
*(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#include "map_reduce.h"
#include "stddefines.h"
#include "utils.h"

#if !defined(_LINUX_) && !defined(_SOLARIS_)
#error OS not supported
#endif
#define TIMING
#define VERBOSE 
/* Debug printf */
#ifdef dprintf
#undef dprintf
#define dprintf(...) //printf(__VA_ARGS__)
#endif

static mr_env_t* env_init(map_reduce_args_t *);
static void env_fini(mr_env_t *env);
void map(mr_env_t *env);
void reduce(mr_env_t *env);

int map_reduce_init()
{
    return 0;
}

int map_reduce(map_reduce_args_t * args)
{
    struct timeval begin;
    struct timeval end;
    mr_env_t* env;
    cl_int error;
    assert(args != NULL);

    get_time(&begin);
    /* Initialize environment. */
    env = env_init(args);
    if(env == NULL) 
    {
       return -1;
    }
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "library init: %ld ms\n", time_diff(&end, &begin));
#endif

    /* Run map tasks and get intermediate values. */
    get_time(&begin);
    map(env);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "map phase: %ld ms\n", time_diff(&end, &begin));
#endif

    // See if we have a valid reduce kernel
    if(env->args->reduce[0] != '\0')
    {
        /* Run reduce tasks and get final values. */
        get_time(&begin);
        reduce(env);
        get_time(&end);
#ifdef TIMING
        fprintf(stderr, "reduce phase: %ld ms\n", time_diff(&end, &begin));
#endif
    }
    else
    {
        // There is no reduce phase, copy the handles
        for(int i = 0; i < env->num_reduce_workgroups; i++)
        {
            // Need to copy the buffer handles
            env->reduce_array[i] = env->map_array[i];
            env->reduce_array_size[i] = env->map_array_size[i];
        }
    }

    // Allocate 1D array for all results combined
    size_t keypair_num = 0;
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        keypair_num += env->reduce_array_size[i];
    }
    void *keyval_array = malloc(env->args->keyval_size * keypair_num);
    void *keyval_ptr = keyval_array;

    // Read back
    get_time(&begin);
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        // keyval_ptr might need to be reference here
        error = clEnqueueReadBuffer(env->device_queue, env->reduce_array[i], CL_FALSE, 0,
            env->reduce_array_size[i] * env->args->keyval_size, keyval_ptr, 0, NULL, NULL);
        keyval_ptr += env->reduce_array_size[i] * env->args->keyval_size;
    }
    clFinish(env->device_queue);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "fetching back from GPU memory: %ld ms\n", time_diff(&end, &begin));
#endif

    // Merge the data 
    merger_dat_t* merg_dat = malloc(sizeof(merger_dat_t));
    merg_dat->keyvals = keyval_array;
    merg_dat->size = keypair_num;
    get_time(&begin);
    env->args->merger(merg_dat);
    get_time(&end);
    // Get the length of resulting data
    *env->args->result_len = merg_dat->output_size;
    env->args->result = merg_dat->output;
#ifdef TIMING
    fprintf(stderr, "merging in CPU: %ld ms\n", time_diff(&end, &begin));
#endif

    /* Cleanup. */
    get_time(&begin);
    env_fini(env);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "library finalize: %ld ms\n", time_diff(&end, &begin));
#endif

    return 0;
}

int map_reduce_finalize()
{
    return 0;
}

/* Setup global state. */
static mr_env_t* env_init(map_reduce_args_t *args) 
{
    mr_env_t    *env;
    cl_int error = 0;
    cl_platform_id platform;
    env = malloc(sizeof(mr_env_t));
    if(env == NULL) 
    {
       return NULL;
    }
    memset(env, 0, sizeof(mr_env_t));
    env->args = args;
    env->buildLogging = false;

    ////////////////////////////////
    /* 1. Init OpenCL enviroment. */
    ////////////////////////////////

    // Platform
    error = oclGetPlatformID(&platform);
    if(error) 
    {
        printf("Error getting platform id");
        exit(error);
    }
    // Device
    error = clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 1, &env->device, NULL);
    if(error) 
    {
        printf("Error getting device ids");
        exit(error);
    }
    // Number of compute units
    clGetDeviceInfo(env->device, CL_DEVICE_MAX_COMPUTE_UNITS, sizeof(env->num_compute_units),
        &env->num_compute_units, NULL);
    if(error) 
    {
        printf("Error getting device info %d", error);
        exit(error);
    }
    fprintf(stderr, "Max compute units: %u\n", env->num_compute_units);
    // Local memory size
    cl_ulong mem_size;
    clGetDeviceInfo(env->device, CL_DEVICE_LOCAL_MEM_SIZE, sizeof(mem_size), &mem_size, NULL);
    if(error) 
    {
        printf("Error getting device info %d", error);
        exit(error);
    }
    fprintf(stderr, "Local mem size: %lu\n", mem_size);
    // Maximum workgroup size
    clGetDeviceInfo(env->device, CL_DEVICE_MAX_WORK_GROUP_SIZE, sizeof(env->max_workitems),
        &env->max_workitems, NULL);
    if(error) 
    {
        printf("Error getting device info %d", error);
        exit(error);
    }
    fprintf(stderr, "Max workgroup size: %zu\n", env->max_workitems);
    // Context
    env->device_context = clCreateContext(NULL , 1, &env->device, NULL, NULL, &error);
    if(error) 
    {
        printf("Error creating context %d", error);
        exit(error);
    }
    // Command-queue
    env->device_queue = clCreateCommandQueue(env->device_context, env->device, 0, &error);
    if(error) 
    {
        printf("Error creating command queue");
        exit(error);
    }

    /////////////////////////////////////
    /* 2. Determine system parameters. */
    /////////////////////////////////////

    if(env->args->num_workgroups > 0)
        env->num_workgroups = env->args->num_workgroups;
    else
        env->num_workgroups = env->num_compute_units;
    if(env->args->num_workitems > 0)
        env->num_workitems = env->args->num_workitems;
    else
        env->num_workitems = env->max_workitems;
    if(env->args->tasks_per_reduce < 1)
        env->args->tasks_per_reduce = 2;
    env->num_reduce_workgroups = env->num_workgroups;
    env->num_reduce_workitems = env->num_workitems / env->args->tasks_per_reduce;

    // No reduce phase, set single task in reduce phase
    if(env->args->reduce[0] == '\0')
    {
        env->args->tasks_per_reduce = 1;
    }

#ifdef VERBOSE
    fprintf(stderr, "Number of map workitems: %zu\n", env->num_workitems);
    fprintf(stderr, "Number of map workgroups: %zu\n", env->num_workgroups);
    fprintf(stderr, "Number of reduce workitems: %zu\n", env->num_reduce_workitems);
    fprintf(stderr, "Number of reduce  workgroups: %zu\n", env->num_reduce_workgroups);
#endif 

    ////////////////////////////////////
    /* 3. Initialize data structures. */
    ////////////////////////////////////

    env->input_array =(cl_mem*)malloc(sizeof(cl_mem) * env->num_workgroups);
    env->map_array =(cl_mem*)malloc(sizeof(cl_mem) * env->num_workgroups);
    env->map_array_size =(cl_uint*)malloc(sizeof(cl_uint) * env->num_workgroups);
    env->map_aux_arg =(cl_mem*)malloc(sizeof(cl_mem));
    env->merged_map_array =(cl_mem*)malloc(sizeof(cl_mem) * env->num_reduce_workgroups);
    env->reduce_array =(cl_mem*)malloc(sizeof(cl_mem) * env->num_reduce_workgroups);
    env->reduce_array_size =(cl_uint*)malloc(sizeof(cl_uint) * env->num_reduce_workgroups);
    env->map_data_size =(cl_uint*)malloc(sizeof(cl_uint) * env->num_workgroups);
    env->reduce_data_size =(cl_uint*)malloc(sizeof(cl_uint) * env->num_reduce_workgroups);
    if(env->args->splitter == NULL)
        env->args->splitter = default_splitter;
    if(env->args->partition == NULL)
        env->args->partition = default_partition;

    return env;
}



/**
 * Frees memory used by map reduce environment once map reduce has completed.
 * Frees environment pointer.
 */
static void env_fini(mr_env_t* env)
{
    cl_int error;

    // Release kernel and program objects
    error = clReleaseKernel(env->map);
    CL_ASSERT(error);
    error = clReleaseProgram(env->map_program);
    CL_ASSERT(error);
    if(env->args->map_count[0] != '\0')
    {
        error = clReleaseKernel(env->map_count);
        CL_ASSERT(error);
        error = clReleaseProgram(env->map_count_program);
        CL_ASSERT(error);
    }
    if(env->args->reduce[0] != '\0')
    {
        error = clReleaseKernel(env->reduce);
        CL_ASSERT(error);
        error = clReleaseProgram(env->reduce_program);
        CL_ASSERT(error);
        if(env->args->reduce_count[0] != '\0')
        {
            error = clReleaseKernel(env->reduce_count);
            CL_ASSERT(error);
            error = clReleaseProgram(env->reduce_count_program);
            CL_ASSERT(error);
        }
    }
    // Release memory objects (note map and merged_map arrays are already gone)
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {    
        error = clReleaseMemObject(env->reduce_array[i]);
        CL_ASSERT(error);
    }
    // Release command queue and context last
    error = clReleaseCommandQueue(env->device_queue);
    CL_ASSERT(error);
    error = clReleaseContext(env->device_context);
    CL_ASSERT(error);

    // Get rid of all dynamic stuff
    free(env->input_array);
    free(env->map_array);
    free(env->merged_map_array);
    free(env->reduce_array);
    free(env->map_array_size);
    free(env->reduce_array_size);
    free(env->map_data_size);
    free(env->reduce_data_size);
    free(env);
}

// Default splitter. Takes the input data and divides it uniformly based on number of tasks
void default_splitter(void* input)
{
    // Partition input into num_workgroups memory buffers
    mr_env_t *env = (mr_env_t*)input;
    cl_uint num_all_tasks = env->args->data_size / env->args->unit_size;
    fprintf(stderr, "Number of tasks: %u\n", num_all_tasks);
    // Calculate buffer sizes and initialize dynamic variables
    cl_uint tasks_per_map = div_round_up(num_all_tasks, env->num_workgroups * env->num_workitems);
    cl_uint tasks_per_group = tasks_per_map * env->num_workitems;
    cl_uint data_len = tasks_per_group * env->args->unit_size;
    void *our_ptr = env->args->task_data;
    int *temp = malloc(sizeof(int));
    env->splitter_data = (splitter_array_t*)malloc(sizeof(splitter_array_t) * env->num_workgroups);

    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        if(env->args->data_size /(tasks_per_group * env->args->unit_size) < 1)
        {
            data_len = env->args->data_size;
        }
        else 
        {
            data_len = tasks_per_group * env->args->unit_size;
        }
        // Some workgroups might get NULL input
        if(env->args->data_size < 1 || data_len < 1)
        {
            data_len = sizeof(int);
            our_ptr = temp;
        }
        env->splitter_data[i].pointer = our_ptr;
        env->splitter_data[i].length = data_len;
        our_ptr += data_len;
        env->args->data_size -= data_len;
    }
}

/**
 * Run the mapper kernel on the GPU
 */
void map(mr_env_t *env)
{
    cl_int error;
    cl_uint temp = 0;
    struct timeval begin;
    struct timeval end;

#ifdef VERBOSE
    fprintf(stderr, "Executing splitter\n");
#endif
    get_time(&begin);
    /* Perform splitter task. */
    env->args->splitter(env);    
    /* Perform splitter task. */
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "splitter: %ld ms\n", time_diff(&end, &begin));
#endif

    /* Perform map task. */
#ifdef VERBOSE
    fprintf(stderr, "init map phase\n");
#endif
    // Build the kernel
    // Check how many pairs splitter produced
    cl_uint num_all_tasks = 0;
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        num_all_tasks += env->splitter_data[i].length / env->args->unit_size;
    }
    cl_uint tasks_per_map = div_round_up(num_all_tasks, env->num_workgroups * env->num_workitems);
    char args[64];
    sprintf(args, "-D TASKS_PER_MAP=%d %s", tasks_per_map, env->args->map_args);

    // Load splitter data to OpenCL buffers
    get_time(&begin);
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        void *inp_ptr = env->splitter_data[i].pointer;
        size_t dat_size = env->splitter_data[i].length;
        env->input_array[i] = clCreateBuffer(env->device_context, 
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, dat_size, inp_ptr, &error);
        CL_ASSERT(error);
        env->map_data_size[i] =(cl_uint)env->splitter_data[i].length;
    }
    // Create aux buffer
    if(env->args->map_aux_size > 0)
    {
        env->map_aux_arg = clCreateBuffer(env->device_context, 
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, env->args->map_aux_size,
            env->args->map_aux_arg, &error);
        CL_ASSERT(error);
    }    
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "Map input buffers init: %ld ms\n", time_diff(&end, &begin));
#endif

    if(env->args->map_count[0] != '\0')
    {
        create_kernel(env, env->args->map_count, &env->map_count_program, &env->map_count, args);
        // Calculate number of work-groups
        cl_mem* output_cnt = malloc(sizeof(cl_mem) * env->num_workgroups);

        // Run map count kernel
        get_time(&begin);
        for(size_t i = 0; i < env->num_workgroups; i++)
        {
            output_cnt[i] =  clCreateBuffer(env->device_context, 
                CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR, sizeof(cl_uint), &temp, &error);
            CL_ASSERT(error);
        }
        for(size_t i = 0; i < env->num_workgroups; i++)
        {
            // Set kernel arguments
            // map input object, intermediate value array(output), map input size
            // Send in pointer to input data and hope kernel can find its chunk
            error = clSetKernelArg(env->map_count, 0, sizeof(env->input_array[i]),
                (void*)&env->input_array[i]);
            error |= clSetKernelArg(env->map_count, 1, sizeof(output_cnt[i]),(void*)&output_cnt[i]);
            error |= clSetKernelArg(env->map_count, 2, sizeof(env->map_data_size[i]),
                (void*)&env->map_data_size[i]);
            if(env->args->map_aux_arg != NULL && env->args->map_aux_size > 0)
            {
                error |= clSetKernelArg(env->map_count, 3, sizeof(env->map_aux_arg),
                    (void*)&env->map_aux_arg);
            }
            CL_ASSERT(error);
            // Enqueue the kernel on the GPU
            error = clEnqueueNDRangeKernel(env->device_queue, env->map_count, 1, NULL,
                &env->num_workitems, &env->num_workitems, 0, NULL, NULL);
            CL_ASSERT(error);
        }
        clFinish(env->device_queue);
        get_time(&end);
#ifdef TIMING
        fprintf(stderr, "map count kernel: %ld ms\n", time_diff(&end, &begin));
#endif

        /////////////////////////////////////////////////////////////
        ////         Atomic dynamic memory allocation            ////
        /////////////////////////////////////////////////////////////
        for(size_t i = 0; i < env->num_workgroups; i++)
        {
            error = clEnqueueReadBuffer(env->device_queue, output_cnt[i], CL_FALSE, 0,
                sizeof(cl_uint), &env->map_array_size[i], 0, NULL, NULL);
            CL_ASSERT(error);
        }
        clFinish(env->device_queue);
        for(size_t i = 0; i < env->num_workgroups; i++)
        {
            // Get rid of the key number counter
            clReleaseMemObject(output_cnt[i]);
            CL_ASSERT(error);
        }
    }
    else
    {
        /////////////////////////////////////////////////////////////
        ////         Static pattern memory allocation            ////
        /////////////////////////////////////////////////////////////
        // Calculate number of key pairs for each workgroup based on input length
        for(size_t i = 0; i < env->num_workgroups; i++)
        {
            cl_uint num_tasks = env->splitter_data[i].length / env->args->unit_size;
            env->map_array_size[i] = num_tasks * env->args->num_output_per_map_task;
        }
    }

    cl_uint num_all_tuples = 0;
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        num_all_tuples += env->map_array_size[i];
    }
#ifdef VERBOSE
    fprintf(stderr, "num of output map tuples: %u\n", num_all_tuples);
#endif

    // Init the buffer for read mapper
    // Intermediate data size depends on amount of key-value pairs returned by all mappers
    // Make some space for intermediate values on the GPU
    // Allow mapreduce arguments to determine size of this buffer
    cl_uint keyval_buffer_size;
    get_time(&begin);
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        keyval_buffer_size = env->map_array_size[i] * env->args->keyval_size;
        // If this buffer is empty, at least put a single keyval into it
        if(keyval_buffer_size == 0)
            keyval_buffer_size = env->args->keyval_size;

        // Create a temp buffer filled with zeroes. Since there is no memset on GPUs and zeroing kernels 
        // don't work properly with big buffers, read the zeroed buffer from main memory instead.
        // This might actually not be needed, as all the buffer should be written to by the kernels.
        //int *temp_buff = malloc(env->map_array_size[i] * env->args->keyval_size);
        //memset(temp_buff, 0, env->map_array_size[i] * env->args->keyval_size);
        //free(temp_buff);

        env->map_array[i] = clCreateBuffer(env->device_context, CL_MEM_READ_WRITE,
            keyval_buffer_size, NULL, &error);
        CL_ASSERT(error);
    }
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "Map output buffers init: %ld ms\n", time_diff(&end, &begin));
#endif
    
    // Build the kernel
    create_kernel(env, env->args->map, &env->map_program, &env->map, args);

    get_time(&begin);
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        error = clSetKernelArg(env->map, 0, sizeof(env->input_array[i]),
            (void*)&env->input_array[i]);
        error |= clSetKernelArg(env->map, 1, sizeof(env->map_array[i]), (void*)&env->map_array[i]);
        error |= clSetKernelArg(env->map, 2, sizeof(env->map_data_size[i]),
            (void*)&env->map_data_size[i]);
        if(env->args->map_aux_arg != NULL && env->args->map_aux_size > 0)
            error |= clSetKernelArg(env->map, 3, sizeof(env->map_aux_arg),(void*)&env->map_aux_arg);
        // Launch the Kernel on the GPU
        error = clEnqueueNDRangeKernel(env->device_queue, env->map, 1, NULL, &env->num_workitems,
            &env->num_workitems, 0, NULL, NULL);
        CL_ASSERT(error);
    }
    clFinish(env->device_queue);
    get_time(&end);

#ifdef TIMING
    fprintf(stderr, "map kernel: %ld ms\n", time_diff(&end, &begin));
#endif
#ifdef VERBOSE
    fprintf(stderr, "calculated map\n");
#endif
    // Release unneeded memory objects
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        error = clReleaseMemObject(env->input_array[i]);
        CL_ASSERT(error);
    }
    if(env->args->map_aux_size > 0)
    {
        error = clReleaseMemObject(env->map_aux_arg);
        CL_ASSERT(error);
    }
}

/**
 * Partition the reduce input
 * 
 */
void default_partition(void* input)
{
    mr_env_t *env =(mr_env_t*)input;
    env->num_reduce_workgroups = env->num_workgroups;
    //cl_int    error;
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        env->merged_map_array[i] = env->map_array[i];
        env->reduce_data_size[i] = env->map_data_size[i];
    }

    // Two-workgroup merged partitoner
    /*// We need to merge proper input for this task
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        cl_uint merged_size = 0;
        for(int j = 0; j < env->args->tasks_per_reduce; j++)
        {
            merged_size += env->map_array_size[i * env->args->tasks_per_reduce + j];
        }
        env->merged_map_array[i] = clCreateBuffer(env->device_context, CL_MEM_READ_WRITE, 
            env->args->keyval_size * merged_size , NULL, &error);
        cl_uint wr_keyvals = 0;
        for(int j = 0; j < env->args->tasks_per_reduce; j++)
        {
            clEnqueueCopyBuffer(env->device_queue, env->map_array[i*env->args->tasks_per_reduce + j],
                env->merged_map_array[i], 0, wr_keyvals, env->args->keyval_size * 
                env->map_array_size[i*env->args->tasks_per_reduce + j], 0, NULL, NULL);

            wr_keyvals += env->args->keyval_size * env->map_array_size[i*env->args->tasks_per_reduce + j];
        }
        // See if this is the last piece and it encounters the last index
        // To avoid memory corruption
        merged_size *= env->args->keyval_size;
        env->reduce_data_size[i] = clCreateBuffer(env->device_context, 
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, sizeof(cl_uint), &merged_size, &error);
        CL_ASSERT(error);
    }
    // Wait for buffers to be properly copied
    clFinish(env->device_queue);

    // Get rid of old buffers
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        clReleaseMemObject(env->map_array[i]);
        CL_ASSERT(error);
    }
    // Wait for buffers to be properly released
    clFinish(env->device_queue);*/
}

void reduce(mr_env_t *env)
{
    cl_int error;
    cl_uint temp = 0;
    struct timeval begin;
    struct timeval end;
    
    /* Perform partition task. */
#ifdef VERBOSE
    fprintf(stderr, "Running partitioner\n");
#endif
    get_time(&begin);
    env->args->partition(env);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "partitioner: %ld ms\n", time_diff(&end, &begin));
#endif

#ifdef VERBOSE
    fprintf(stderr, "init reduce phase\n");
#endif
    cl_uint tasks_per_reduce = div_round_up(env->map_array_size[0], env->num_reduce_workitems);
    char args[64];
    sprintf(args, "-D TASKS_PER_REDUCE=%d %s", tasks_per_reduce, env->args->reduce_args);
    // Calculate number of work-groups
    cl_mem* output_cnt = malloc(sizeof(cl_mem) * env->num_reduce_workgroups);
    // Build the reduce count kernel
    create_kernel(env, env->args->reduce_count, &env->reduce_count_program, &env->reduce_count, args);

    get_time(&begin);
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        output_cnt[i] =  clCreateBuffer(env->device_context, 
            CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR, sizeof(cl_uint), &temp, &error);
        CL_ASSERT(error);
        error = clSetKernelArg(env->reduce_count, 0, sizeof(env->merged_map_array[i]),
            (void*)&env->merged_map_array[i]);
        error |= clSetKernelArg(env->reduce_count, 1, sizeof(output_cnt[i]),(void*)&output_cnt[i]);
        error |= clSetKernelArg(env->reduce_count, 2, sizeof(env->reduce_data_size[i]),
            (void*)&env->reduce_data_size[i]);
        CL_ASSERT(error);

        // Launch the Kernel on the GPU
        error = clEnqueueNDRangeKernel(env->device_queue, env->reduce_count, 1, NULL,
            &env->num_reduce_workitems, &env->num_reduce_workitems, 0, NULL, NULL);
        CL_ASSERT(error);
    }
    clFinish(env->device_queue);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "reduce count kernel: %ld ms\n", time_diff(&end, &begin));
#endif
    
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        error = clEnqueueReadBuffer(env->device_queue, output_cnt[i], CL_FALSE, 0, sizeof(cl_uint),
            &env->reduce_array_size[i], 0, NULL, NULL);
        CL_ASSERT(error);
    }
    clFinish(env->device_queue);

    // Get rid of the key number counters
    for(size_t i = 0; i < env->num_workgroups; i++)
    {
        clReleaseMemObject(output_cnt[i]);
        CL_ASSERT(error);
    }

    cl_uint num_all_tuples = 0;
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        num_all_tuples += env->reduce_array_size[i];
    }
#ifdef VERBOSE
    fprintf(stderr, "calculated reduce_count\nnum of tuples: %u\n", num_all_tuples );
#endif

    cl_uint keyval_buffer_size;
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        keyval_buffer_size = env->reduce_array_size[i] * env->args->keyval_size;
        if(keyval_buffer_size == 0)
            keyval_buffer_size = env->args->keyval_size;
            
        // Temp buffer to read zeroed memory onto GPU
        //int *temp_buff = malloc(env->reduce_array_size[i] * env->args->keyval_size);
        //memset(temp_buff, 0, env->reduce_array_size[i] * env->args->keyval_size);
        //free(temp_buff);
        
        env->reduce_array[i] = clCreateBuffer(env->device_context, CL_MEM_READ_WRITE, 
            keyval_buffer_size, NULL, &error);
        CL_ASSERT(error);
    }

    // Build the reduce kernel
    create_kernel(env, env->args->reduce, &env->reduce_program, &env->reduce, args);

    get_time(&begin);
    // Set kernel arguments
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        error = clSetKernelArg(env->reduce, 0, sizeof(env->merged_map_array[i]),
            (void*)&env->merged_map_array[i]);
        error |= clSetKernelArg(env->reduce, 1, sizeof(env->reduce_array[i]),
            (void*)&env->reduce_array[i]);
        error |= clSetKernelArg(env->reduce, 2, sizeof(env->reduce_data_size[i]),
            (void*)&env->reduce_data_size[i]);
        CL_ASSERT(error);

        // Launch the Kernel on the GPU
        error = clEnqueueNDRangeKernel(env->device_queue, env->reduce, 1, NULL, 
            &env->num_reduce_workitems, &env->num_reduce_workitems, 0, NULL, NULL);
        CL_ASSERT(error);
    }
    clFinish(env->device_queue);
    get_time(&end);
#ifdef TIMING
    fprintf(stderr, "reduce kernel: %ld ms\n", time_diff(&end, &begin));
#endif
#ifdef VERBOSE
    fprintf(stderr, "calculated reduce\n");
#endif

    // Release memory object
    for(int i = 0; i < env->num_reduce_workgroups; i++)
    {
        clReleaseMemObject(env->merged_map_array[i]);
        CL_ASSERT(error);
    }
}
