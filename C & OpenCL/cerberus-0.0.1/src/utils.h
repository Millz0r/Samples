/*  Karol Pogonowski - Master of Informatics Dissertation
	This is an OpenCL MapReduce library written in C/OpenCL and primarily
	targeting GPU computing.
*/

#ifndef MAP_UTILS_H_
#define MAP_UTILS_H_

#include "map_reduce.h"
#include "string.h"

unsigned int div_round_up(unsigned int x, unsigned int y);
void create_kernel(mr_env_t* env, const char* path, cl_program* program, cl_kernel* kernel,
	const char* flags);
char* get_kernel_name(const char* path);
cl_int oclGetPlatformID(cl_platform_id* clSelectedPlatformID);
char* oclLoadProgSource(const char* cFilename, size_t* szFinalLength);

#endif
