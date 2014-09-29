// Map

#define ROUND_UP(a, b) (a + b - 1)/b

enum {
    KEY_SX = 0,
    KEY_SY,
    KEY_SXX,
    KEY_SYY,
    KEY_SXY
};

typedef struct
{
	int key;
	long value;
} keyval_t;

__kernel void linear_map( __global const char2* input, __global keyval_t* output,
							uint data_size)
{
	uint idx = get_local_id(0);

	uint last_index = ROUND_UP(data_size, sizeof(char2));

	// input[idx] points to beginning of our data
	// Calculate number of tasks
	// Get data length and divide by input 
	uint task_count = 0;
	uint curr_idx;
	long x, y;
	long sx = 0, sxx = 0, sy = 0, syy = 0, sxy = 0;
	keyval_t temp;
	
	for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++)
	{	
		curr_idx = idx + get_local_size(0) * task_count;
		
		if (curr_idx >= last_index)
			break;
			
		x = (long)input[curr_idx].x;
		y = (long)input[curr_idx].y;

		//Compute SX, SY, SYY, SXX, SXY
		sx  += x;
		sxx += x * x;
		sy  += y;
		syy += y * y;
		sxy += x * y;
	}
	// Do partial reduction in map
		
	uint current;

	current = idx + (get_local_size(0) * 0);
	temp.key = KEY_SX;
	temp.value = sx;
	output[current] = temp;
	
	current = idx + (get_local_size(0) * 1);
	temp.key = KEY_SXX;
	temp.value = sxx;
	output[current] = temp;
	
	current = idx + (get_local_size(0) * 2);
	temp.key = KEY_SY;
	temp.value = sy;
	output[current] = temp;
	
	current = idx + (get_local_size(0) * 3);
	temp.key = KEY_SYY;
	temp.value = syy;
	output[current] = temp;
	
	current = idx + (get_local_size(0) * 4);
	temp.key = KEY_SXY;
	temp.value = sxy;
	output[current] = temp;
}
