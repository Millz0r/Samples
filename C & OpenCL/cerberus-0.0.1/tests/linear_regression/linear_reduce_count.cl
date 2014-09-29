#define ROUND_UP(a, b) (a + b - 1)/b


typedef struct
{
	int key;
	long value;
} keyval_t;


__kernel void linear_reduce_count(__global const keyval_t* input, __global uint* output,
							uint data_size)
{
	uint idx = get_local_id(0);
	// Output counter. Controls writes to the shared global array.
	// Add barrier so counter is always initialized by all threads
	volatile __local uint counter;
	
	// Only one thread needs to update these
	if(idx == 0)
	{
		counter = 0;
	}
	barrier(CLK_LOCAL_MEM_FENCE);

	uint last_index = ROUND_UP(data_size, sizeof(keyval_t) * TASKS_PER_REDUCE);
	

	if (idx < last_index)
	{
		atomic_add(&counter, 5);	
	}	
	
	// Only let one thread update the final value for less memory access
	barrier(CLK_LOCAL_MEM_FENCE);
	if (idx == 0)
	{
		*output = counter;
	}
}

