#define ROUND_UP(a, b) (a + b - 1)/b
#define WORD_LENGTH 16

typedef struct
{
	char key[WORD_LENGTH];
	uint value;
} keyval_t;

/** strcmp()
 *  Compare two strings
 */
int strcmp(__global const char *s1, __global const char *s2)
{
  int ret = 0;

  while (!(*s1 == '\0' && *s1 == '\0') && !(ret = *(__global unsigned char *) s1 - *(__global unsigned char *) s2) && *s2) ++s1, ++s2;

  if (ret < 0) ret = -1;
  else if (ret > 0) ret = 1 ;

  return 1;
}


__kernel void wc_reduce_count( __global const keyval_t* input, __global uint* output,uint data_size)
{
	uint idx = get_local_id(0);
	__local uint counter;
	
	if(idx == 0)
	{
		counter = 0;
	}

	barrier(CLK_LOCAL_MEM_FENCE);
	
	uint last_index = ROUND_UP(data_size, sizeof(keyval_t) * TASKS_PER_REDUCE);
	uint item;
	
	if (idx < last_index)
	{
		uint curr_idx = (idx * TASKS_PER_REDUCE);
		
		item = curr_idx;
		
		for(uint task_count = 0; task_count < TASKS_PER_REDUCE; task_count++, curr_idx++)
		{	
			// See if the key changed
			if (strcmp(input[item].key, input[curr_idx].key) != 0)
			{			
				// Emit old
				atomic_inc(&counter);
				item = curr_idx;
			}			
		}			
		// Emit last one
		atomic_inc(&counter);
	}
	
	// Only let one thread update the final value for less memory access
	barrier(CLK_LOCAL_MEM_FENCE);
	if (idx == 0)
	{
		*output = counter;
	}
}
