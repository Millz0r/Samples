#define ROUND_UP(a, b) (a + b - 1)/b

typedef struct
{
	float key;
	int2 value;
} keyval_t;

__kernel void ss_map(__global const int2* input, __global keyval_t* output, uint data_size, 
						__global const float4* matrix)
{
	uint idx = get_local_id(0);
	uint last_index = ROUND_UP(data_size, sizeof(int2));
	int2 curr;
	keyval_t temp;
	uint curr_idx;
	
	float up = 0.0f;
	float down = 0.0f;
	float result = 0.0f;
	int doc1;
	int doc2;
	float doc1Down = 0.0f;
	float doc2Down = 0.0f;
	int col4 = VEC_SIZE >> 2;
	int remainder = VEC_SIZE & 3;
	float4 aValue;
	float4 bValue;
	int i;
		
	for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++)
	{	
		// Coalesced read
		curr_idx = idx + get_local_size(0) * task_count;
		if (curr_idx >= last_index)
			break;
			
		curr = input[curr_idx];	
		doc1 = curr.x;
		doc2 = curr.y;
		
		// Load our vectors
		__global const float4 *a = (__global const float4*)(matrix + doc1*VEC_SIZE);
		__global const float4 *b = (__global const float4*)(matrix + doc2*VEC_SIZE);
		
		__global const float *a1 = (__global const float*)(a + col4);
		__global const float *b1 = (__global const float*)(b + col4);
		
		for (i = 0; i < col4; i++)
		{
			aValue = a[i]; 
			bValue = b[i];
			
			up += (aValue.x *bValue.x+aValue.y *bValue.y+aValue.z *bValue.z+aValue.w *bValue.w);
			
			doc1Down += aValue.x*aValue.x+aValue.y*aValue.y+aValue.z*aValue.z+aValue.w*aValue.w;
			doc2Down += bValue.x*bValue.x+bValue.y*bValue.y+bValue.z*bValue.z+bValue.w*bValue.w;
		}

		for (i = 0; i < remainder; i++)
		{
			up += (a1[i] * b1[i]);
			doc1Down += (a1[i]*a1[i]);
			doc2Down += (b1[i]*b1[i]);
		}
		
		down = sqrt(doc1Down) * sqrt(doc2Down);
		result = up / down;

		temp.key = result;
		temp.value = curr;
		
		uint current = idx + (get_local_size(0) * task_count);
		output[current] = temp;
	}
}

