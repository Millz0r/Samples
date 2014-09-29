// Map

#define ROUND_UP(a, b) (a + b - 1)/b

typedef struct
{
	int key;
	uint value;
} keyval_t;

typedef struct
{
	uchar r;
	uchar g;
	uchar b;
} rgb_t;

__kernel void hist_map( __global const rgb_t* input, __global keyval_t* output, uint data_size)
{
	uint idx = get_local_id(0);
	uint last_index = ROUND_UP(data_size, sizeof(rgb_t));
	keyval_t temp;
	rgb_t curr;
	uint curr_idx;

	for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++)
	{		
		// Coalesced read, 1 stride distance between thread accesses
		curr_idx = idx + get_local_size(0) * task_count;
		if (curr_idx >= last_index)
			break;
			
		curr = input[curr_idx];
		uint current;
			
		// Write to global memory
		temp.key = curr.r;
		temp.value = 1;
		current = idx + get_local_size(0) * (0 + 3 * task_count);
		output[current] = temp;
		
		temp.key = curr.g+256;
		temp.value = 1;
		current = idx + get_local_size(0) * (1 + 3 * task_count);
		output[current] = temp;
					
		temp.key = curr.b+512;
		temp.value = 1;
		current = idx + get_local_size(0) * (2 + 3 * task_count);
		output[current] = temp;
	}

}
