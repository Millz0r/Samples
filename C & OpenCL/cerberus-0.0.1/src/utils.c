/*  Karol Pogonowski - Master of Informatics Dissertation
	This is an OpenCL MapReduce library written in C/OpenCL and primarily
	targeting GPU computing.
*/

#include "stddefines.h"
#include "utils.h"

//==========================================//
//											//
// Auxiliary helper functions				//
//											//
//==========================================//

unsigned int div_round_up(unsigned int x, unsigned int y) 
{ 
	// To avoid overflow
	if (y >= x)
		return 1;
		
	return ((x + y - 1) / y);
}

char* getKernelName(const char* path)
{
	int len = strlen(path);
	int  num_chars = 0;
	int start;
	int end;
	// Find the end '.cl' sequence
	for(end = len - 1; end >= 0; end--)
	{
		char temp = path[end];

		if(temp == '.')
			break;
	}
	end--;

	// and iterate until beginning of string or 
	for(start = end; start > 0; start--)
	{
		char temp = path[start];
		if(temp == '\\' || temp == '/')
			break;
	}

	num_chars = end - start + 1;
	char* out = (char*)malloc(sizeof(char) * (num_chars + 1));

	strncpy(out, &path[start], num_chars);
	out[num_chars] = '\0';

	return out;
}

void createKernel(mr_env_t* env, const char* path, cl_program* program, cl_kernel* kernel, const char* flags)
{
	// Build kernel	cl_int error;
	const char* name = getKernelName(path);
	size_t src_size = 0;
	const char *source = oclLoadProgSource(path, &src_size);
	cl_int error;

	*program = clCreateProgramWithSource(env->device_context, 1, &source, &src_size, &error);

	if (error) 
	{
		fprintf (stderr, "Error creating %s program: %d", name, error);
		exit(error);
	}

	// Builds the program
	error = clBuildProgram(*program, 1, &env->device, flags, NULL, NULL);
	if (error) 
	{
		fprintf (stderr, "Error building %s program: %d\n", name, error);
		size_t len;
		char *buffer;
		clGetProgramBuildInfo(*program, env->device, CL_PROGRAM_BUILD_LOG, 0, NULL, &len);
		buffer = malloc(len);
		clGetProgramBuildInfo(*program, env->device, CL_PROGRAM_BUILD_LOG, len, buffer, NULL);
		printf("%s\n", buffer);
		exit(error);
	}

	// Extracting the kernel
	*kernel = clCreateKernel(*program, name, &error);
	if (error) 
	{
		fprintf (stderr, "Error creating %s kernel: %d", name, error);
		exit(error);
	}

	if (env->buildLogging)
	{
		// Store the logs
		char* build_log;
		size_t log_size;

		// First call to know the proper size
		clGetProgramBuildInfo(*program, env->device, CL_PROGRAM_BUILD_LOG, 0, NULL, &log_size);
		build_log = malloc(sizeof(char) * (log_size+1));

		// Second call to get the log
		clGetProgramBuildInfo(*program, env->device, CL_PROGRAM_BUILD_LOG, log_size, build_log, NULL);
		build_log[log_size] = '\0';

		//logWriter << build_log << endl;
		free(build_log);
	}
		
	size_t ret;
	error = clGetKernelWorkGroupInfo( *kernel, env->device, CL_KERNEL_WORK_GROUP_SIZE, sizeof(size_t), &ret, NULL);
	CL_ASSERT(error);
	
	fprintf (stderr, "Kernel %s specific maximum workgroup size %zu\n", name, ret);
	
	if (ret < env->num_workitems)
		fprintf (stderr, "Error - too many map workitems\n");
		
		
	if (ret < env->num_reduce_workitems)
		fprintf (stderr, "Error - too many reduce workitems\n");
}


// Those functions are taken from util.cpp in NVIDIA GPGPU SDK
cl_int oclGetPlatformID(cl_platform_id* clSelectedPlatformID)
{
	char chBuffer[1024];
	cl_uint num_platforms; 
	cl_platform_id* clPlatformIDs;
	cl_int ciErrNum;

	// Get OpenCL platform count
	ciErrNum = clGetPlatformIDs (0, NULL, &num_platforms);
	if (ciErrNum != CL_SUCCESS)
	{
		//shrLog(LOGBOTH, 0, " Error %i in clGetPlatformIDs Call !!!\n\n", ciErrNum);
		return -1000;
	}
	else 
	{
		if(num_platforms == 0)
		{
			//shrLog(LOGBOTH, 0, "No OpenCL platform found!\n\n");
			return -2000;
		}
		else 
		{
			// if there's a platform or more, make space for ID's
			if ((clPlatformIDs = (cl_platform_id*)malloc(num_platforms * sizeof(cl_platform_id))) == NULL)
			{
				//shrLog(LOGBOTH, 0, "Failed to allocate memory for cl_platform ID's!\n\n");
				return -3000;
			}

			// get platform info for each platform and trap the NVIDIA platform if found
			ciErrNum = clGetPlatformIDs (num_platforms, clPlatformIDs, NULL);
			for(cl_uint i = 0; i < num_platforms; ++i)
			{
				ciErrNum = clGetPlatformInfo (clPlatformIDs[i], CL_PLATFORM_NAME, 1024, &chBuffer, NULL);
				if(ciErrNum == CL_SUCCESS)
				{
					if(strstr(chBuffer, "NVIDIA") != NULL)
					{
						*clSelectedPlatformID = clPlatformIDs[i];
						break;
					}
				}
			}

			// default to zeroeth platform if NVIDIA not found
			if(*clSelectedPlatformID == NULL)
			{
				//shrLog(LOGBOTH, 0, "WARNING: NVIDIA OpenCL platform not found - defaulting to first platform!\n\n");
				*clSelectedPlatformID = clPlatformIDs[0];
			}

			free(clPlatformIDs);
		}
	}

	return CL_SUCCESS;
}

char* oclLoadProgSource(const char* cFilename, size_t* szFinalLength)
{
	// locals 
	FILE* pFileStream = NULL;
	size_t szSourceLength;

	// open the OpenCL source code file
	pFileStream = fopen(cFilename, "rb");
	if(pFileStream == 0) 
	{       
		fprintf(stderr, "\nExiting. Cannot open file %s!", cFilename);
		exit(0);
		return NULL;
	}

	// get the length of the source code
	fseek(pFileStream, 0, SEEK_END); 
	szSourceLength = ftell(pFileStream);
	fseek(pFileStream, 0, SEEK_SET); 

	// allocate a buffer for the source code string and read it in
	char* cSourceString = (char *)malloc(szSourceLength + 1); 
	if (fread((cSourceString), szSourceLength, 1, pFileStream) != 1)
	{
		fclose(pFileStream);
		free(cSourceString);
		return 0;
	}

	// close the file and return the total length of the combined (preamble + source) string
	fclose(pFileStream);
	if(szFinalLength != 0)
	{
		*szFinalLength = szSourceLength;
	}
	cSourceString[szSourceLength] = '\0';

	return cSourceString;
}
