// Map

typedef struct
{
	int key;
	uint value;
} keyval_t;

__kernel void hist_reduce_count( __global keyval_t* input, __global uint* output, 
				uint data_size)
{
	uint idx = get_local_id(0);

	if (idx == 0)
	{
		// Here, we know that the number of reduce pairs is static and equal to 768
		// Only let the first thread update that
		*output = 768;
	}
}
