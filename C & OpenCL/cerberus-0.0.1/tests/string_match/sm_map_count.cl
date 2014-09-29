#define ROUND_UP(a, b) (a + b - 1)/b

#define WORD_LENGTH 16
#define LINE_LENGTH 128

#define WORD1 "Helloworld"
#define WORD2 "howareyou"
#define WORD3 "ferrari"
#define WORD4 "whotheman"


enum {
    IN_WORD,
    NOT_IN_WORD
};

typedef struct
{
	char x[LINE_LENGTH];
} input_t;

typedef struct
{
	char x[WORD_LENGTH];
} word_t;

typedef struct
{
	int key;
	int value;
} keyval_t;

int is_letter(const char curr_ltr)
{
	if ((curr_ltr >= 'A' && curr_ltr <= 'Z') || (curr_ltr >= 'a' && curr_ltr <= 'z'))
		return 1;
	return 0;
}

/** strcmp()
 *  Standard string compare function
 */
int strcmp(__constant const char* s1, const char* s2, const uint len)
{
	int ret = 0;
	uint i = 0;
	
	while (i < len && !(ret = *(__constant const uchar*) s1 - *(const uchar*) s2) && *s2) ++s1,++s2,++i;
	
	if (ret < 0)
		ret = -1;
	else if (ret > 0)
		ret = 1 ;

	return ret;
}


__kernel void sm_map_count( __global const input_t* input, __global uint* output, uint data_size)
{
	uint idx = get_local_id(0);
	__local uint counter;
		
	if(idx == 0)
	{
		counter = 0;
	}
	
	barrier(CLK_LOCAL_MEM_FENCE);
	
	uint last_index = ROUND_UP(data_size, sizeof(input_t));
	uint curr_idx;
	keyval_t temp;
	input_t buffer;
	
	for(uint task_count = 0; task_count < TASKS_PER_MAP; task_count++)
	{		
		curr_idx = idx + get_local_size(0) * task_count;		
		if (curr_idx >= last_index)
			break;
			
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
			curr_ltr = buffer.x[i];
			switch (state)
			{
				case IN_WORD:
					// End of word detected
					if (is_letter(curr_ltr) == 0)
					{
						uint len = &buffer.x[i] - curr_start;
						
						if(!strcmp(WORD1, curr_start, len))
						{
							atomic_inc(&counter);
						}
							
						if(!strcmp(WORD2, curr_start, len))
						{		
							atomic_inc(&counter);
						}
							
						if(!strcmp(WORD3, curr_start, len))
						{		
							atomic_inc(&counter);
						}
							
						if(!strcmp(WORD4, curr_start, len))
						{		
							atomic_inc(&counter);
						}
						
						state = NOT_IN_WORD;
					}
					break;

			default:
				case NOT_IN_WORD:
					if (is_letter(curr_ltr) == 1)
					{
						curr_start = &buffer.x[i];
						state = IN_WORD;
					}
					break;
			}
		}

		// Add the last word
		if (state == IN_WORD)
		{		
			uint len = &buffer.x[i] - curr_start;

			if(!strcmp(WORD1, curr_start, len))
			{
				atomic_inc(&counter);
			}
				
			if(!strcmp(WORD2, curr_start, len))
			{		
				atomic_inc(&counter);
			}
				
			if(!strcmp(WORD3, curr_start, len))
			{		
				atomic_inc(&counter);
			}
				
			if(!strcmp(WORD4, curr_start, len))
			{		
				atomic_inc(&counter);
			}
		}
	}
	
	barrier(CLK_LOCAL_MEM_FENCE);
	if (idx == 0)
	{
		*output = counter;
	}
}    
	
