// Reduce

#define ROUND_UP(a, b) (a + b - 1)/b
typedef struct
{
	int key;
	uint value;
} keyval_t;


__kernel void hist_reduce( __global const keyval_t* input, __global keyval_t* output, uint data_size)
{
	uint idx = get_local_id(0);
	__local uint hist_vals[768];

	if( idx == 0)
	{
		// Init hist counters
		for(uint i = 0; i < 768; i++)
			hist_vals[i] = 0;
	}
	barrier(CLK_LOCAL_MEM_FENCE);
		
	uint last_index = ROUND_UP(data_size, sizeof(keyval_t));
	

	uint task_count = 0;
	uint curr_idx;
	
	for(uint task_count = 0; task_count < TASKS_PER_REDUCE; task_count++)
	{	
		// Coalesced read
		curr_idx = idx + get_local_size(0) * task_count;
		if (curr_idx >= last_index)
			break;
			
		int key = input[curr_idx].key;
		if (key < 768)
		{
			atomic_inc(&hist_vals[key]);
		}
	}		

	// Commit each single pair to global memory
	// This might not be needed, besides coalescing
	barrier(CLK_LOCAL_MEM_FENCE);
	if (idx == 0)
	{		
		keyval_t temp;
		for(int i = 0; i < 768; i++)
		{
			temp.key = i;
			temp.value = hist_vals[i];
			output[i] = temp;
		}
	}

}
