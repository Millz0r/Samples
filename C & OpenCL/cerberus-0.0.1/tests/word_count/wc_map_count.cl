
#define ROUND_UP(a, b) (a + b - 1)/b

#define WORD_LENGTH 16
#define LINE_LENGTH 128

typedef struct
{
	char x[LINE_LENGTH];
} input_t;

typedef struct
{
	char key[WORD_LENGTH];
	uint value;
} keyval_t;

enum {
    IN_WORD,
    NOT_IN_WORD
};

char to_upper(char curr_ltr)
{
	if (curr_ltr >= 'a' && curr_ltr <= 'z')
		curr_ltr -= 32;
		
	return curr_ltr;
}

__kernel void wc_map_count( __global const input_t* input, __global uint* output, uint data_size)
{
	uint idx = get_local_id(0);
	// Output counter. Controls writes to the shared global array.
	// Add barrier so counter is always initialized by all threads
	__local uint counter;
	
	// Only one thread needs to update these
	if(idx == 0)
	{
		counter = 0;
	}

	barrier(CLK_LOCAL_MEM_FENCE);

	uint last_index = ROUND_UP(data_size, sizeof(input_t) * TASKS_PER_MAP);
	
	if (idx < last_index)
	{
		uint curr_idx = (idx * TASKS_PER_MAP);
		input_t buffer;
		
		for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++, curr_idx++)
		{		
			char curr_ltr;
			int state = NOT_IN_WORD;
			int i;
			buffer = input[curr_idx];
									
			// Check for NULL input
			if (buffer.x[0] == '\0')
				return;
					
			for (i = 0; i < LINE_LENGTH; i++)
			{					
				curr_ltr = to_upper(buffer.x[i]);
				switch (state)
				{
					case IN_WORD:
						// Write back with uppercase
						buffer.x[i] = curr_ltr;
						// End of word detected
						if ((curr_ltr < 'A' || curr_ltr > 'Z') && curr_ltr != '\'')
						{
							atomic_inc(&counter);	
							state = NOT_IN_WORD;
						}
						break;

				default:
					case NOT_IN_WORD:
						if (curr_ltr >= 'A' && curr_ltr <= 'Z')
						{
							buffer.x[i] = curr_ltr;
							state = IN_WORD;
						}
						break;
				}
			}

			// Add the last word
			if (state == IN_WORD)
			{
				atomic_inc(&counter);				
			}
		}
	}
	// Only let one thread update the final value for less memory access
	barrier(CLK_LOCAL_MEM_FENCE);
	if (idx == 0)
	{
		*output = counter;
	}
}  
  


