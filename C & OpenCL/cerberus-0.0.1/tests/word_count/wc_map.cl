
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

/** strcpy()
 *  Copy part of in buffer to out
 */
void strcpy(char* out, const char* in, uint n)
{
	uint i;
	// Overflow prevention
	if (n > WORD_LENGTH)
		n = WORD_LENGTH;
		
	for(i = 0; i < n; i++)
	{
		out[i] = in[i];
		// Exit if null ended
		if (in[i] == '\0')
			return;
	}
	if (i < WORD_LENGTH - 1)
	{
		// Put a NULL at the end
		i++;
		out[i] = '\0';
	}
}

char to_upper(char curr_ltr)
{
	if (curr_ltr >= 'a' && curr_ltr <= 'z')
		curr_ltr -= 32;
		
	return curr_ltr;
}

__kernel void wc_map( __global const input_t* input, __global keyval_t* output, uint data_size)
{
	uint idx = get_local_id(0);
	__local uint counter;
	
	if(idx == 0)
	{
		counter = 0;
	}

	barrier(CLK_LOCAL_MEM_FENCE);

	uint last_index = ROUND_UP(data_size, sizeof(input_t) * TASKS_PER_MAP);
	
	if (idx < last_index)
	{
		uint curr_idx = (idx * TASKS_PER_MAP);
		keyval_t temp;
		input_t buffer;
	
		for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++, curr_idx++)
		{		
			char* curr_start;
			char curr_ltr;
			int state = NOT_IN_WORD;
			int i;
			buffer = input[curr_idx];
						
			curr_start = &buffer.x;
			
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
							temp.value = 1;
							// Emit
							// Update the output array counter
							uint current = atomic_inc(&counter);
							strcpy(&temp.key, curr_start, &buffer.x[i] - curr_start + 1);
							output[current] = temp;
							state = NOT_IN_WORD;
						}
						break;

				default:
					case NOT_IN_WORD:
						if (curr_ltr >= 'A' && curr_ltr <= 'Z')
						{
							curr_start = &buffer.x[i];
							buffer.x[i] = curr_ltr;
							state = IN_WORD;
						}
						break;
				}
			}

			// Add the last word
			if (state == IN_WORD)
			{
				// Update the output array counter
				// Emit
				// Update the output array counter
				strcpy(&temp.key, curr_start, &buffer.x[i] - curr_start);
				// Add null ending
				temp.key[&buffer.x[i] - curr_start] = '\0';
				temp.value = 1;
				
				uint current = atomic_inc(&counter);				
				output[current] = temp;
			}
		}
	}
}  

