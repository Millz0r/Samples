// Map

#define ROUND_UP(a, b) (a + b - 1)/b

typedef struct
{
	uint v1;
	uint v2;
} input_t;

typedef struct
{
	uint key;
	int value;
} keyval_t;

__kernel void mm_map( __global const input_t* input, __global keyval_t* output, uint data_size, 
					__global const int* matrix)		
{
	uint idx = get_local_id(0);

	uint last_index = ROUND_UP(data_size, sizeof(input_t));
	keyval_t temp;
	input_t curr;
	uint curr_idx;
	
	for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++)
	{	
		curr_idx = idx + get_local_size(0) * task_count;
		if (curr_idx >= last_index)
			break;
			
		curr = input[curr_idx];
		int value = 0;
		int a, b;
		uint i = curr.v1;
		uint j = curr.v2;
					
		__global const int* m_a = matrix;
		__global const int* m_b = matrix + ROW_NUM*ROW_NUM;
				
		for(uint k = 0; k < ROW_NUM; k++)
		{
			// Sum[ A[ik] * B[kj]]
			a = m_a[i * ROW_NUM + k];
			
			b = m_b[k * ROW_NUM + j];

			value += a * b;
		}
		
		// Coalesced write
		uint current = idx + (get_local_size(0) * task_count);
		
		temp.key = i * ROW_NUM + j;
		temp.value = value;
		
		output[current] = temp;
	}
}