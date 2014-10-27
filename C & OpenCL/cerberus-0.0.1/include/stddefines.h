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
*     * Neither the name of Stanford University nor the
*       names of its contributors may be used to endorse or promote products
*       derived from this software without specific prior written permission.
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

#ifndef STDDEFINES_H_
#define STDDEFINES_H_

#include <assert.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>

//#define TIMING

/* Debug printf */
#define dprintf(...) fprintf(stdout, __VA_ARGS__)

/* Wrapper to CL error check */
#define CL_ASSERT(a)                                 \
   if(a != CL_SUCCESS)                               \
   {                                                 \
      fprintf(stderr, "OpenCL fatal error: %d", a);  \
      exit(0);                                     	 \
   }

/* Wrapper to check for errors */
#define CHECK_ERROR(a)                               \
   if(a)                                             \
   {                                                 \
      perror("Error at line\n\t" #a "\nSystem Msg"); \
      assert((a) == 0);                              \
   }

static inline long time_diff(struct timeval *end, struct timeval *begin)
{	
    long mtime, seconds, useconds;   
    seconds  = end->tv_sec - begin->tv_sec;
    useconds = end->tv_usec - begin->tv_usec;
    mtime = ((seconds) * 1000 + useconds/1000.0) + 0.5;	

    return mtime;
}

static inline void get_time(struct timeval *t)
{
    gettimeofday(t, NULL);
}

#endif // STDDEFINES_H_
