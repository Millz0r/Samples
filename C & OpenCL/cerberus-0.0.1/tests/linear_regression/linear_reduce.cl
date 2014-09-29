// Reduce

#define ROUND_UP(a, b) (a + b - 1)/b

typedef struct
{
	int key;
	long value;
} keyval_t;

__kernel void linear_reduce( __global const keyval_t* input, __global keyval_t* output,
			uint data_size)
{
	uint idx = get_local_id(0);
	
	uint last_index = ROUND_UP(data_size, sizeof(keyval_t));

	// input[idx] points to beginning of our data
	// Calculate number of tasks
	// Get data length and divide by input 
	uint task_count = 0;

	long sum = 0;
	uint curr_idx;
	keyval_t temp;
	long lr_vals[5];

	for(int i = 0; i < 5; i++)
	{
		lr_vals[i] = 0;
	}
	
	for(uint task_count = 0; task_count < TASKS_PER_REDUCE; task_count++)
	{	
		// Would be good to get all this chunk to local memory
		// Coalesced read
		curr_idx = idx + get_local_size(0) * task_count;
		if (curr_idx >= last_index)
			break;
			
		temp = input[curr_idx];
		lr_vals[temp.key] += temp.value;
	}	

	uint current;// = atomic_add(&counter, 5);
	for(int i = 0; i < 5; i++)
	{
		temp.key = i;
		temp.value = lr_vals[i];
		current = idx + get_local_size(0) * i;
		output[current] = temp;
	}		
}

