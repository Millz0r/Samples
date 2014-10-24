import sys
from difflib import Differ, ndiff


class colours:
    GREY = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    ENDC = '\033[0m'


# Enclose diffs in specific console colours codes and remove unneeded newlines
def pretty_print(diff_text):
    for item in diff_text:
        if item.startswith('+'):
            print colours.GREEN + item[:-1] + colours.ENDC
        elif item.startswith('-'):
            print colours.RED + item[:-1] + colours.ENDC
        else:
            print colours.GREY + item[:-1] + colours.ENDC


# Add diff-truncated string to the tail of output string
def append(index):
    global output
    # Pass the two-letter diff code
    data = diff_result[index][2:]
    output.append(data)


# Returns min if both numbers are not -1
def min_not_one(num1, num2):
    if num1 == -1:
        return num2
    elif num2 == -1:
        return num1
    else:
        return min(num1, num2)


def show_context(i, mode):
    buffer = ''
    print
    for j in range(i-1, 0, -1):
        if diff_result[j].startswith(mode) or diff_result[j].startswith(' '):
            if '    - ' in diff_result[j]:
                if diff_result[j-1][:-1].startswith('?'):
                    buffer = diff_result[j][:-1]
                    continue
                else:
                    buffer = diff_result[j][:-1]
                    continue
            print diff_result[j][:-1]
            break
    if buffer:
        print buffer
    print colours.GREEN + diff_result[i][:-1] + colours.ENDC
    for j in range(i+1, len(diff_result)):
        if diff_result[j].startswith(mode) or diff_result[j].startswith(' '):
            print diff_result[j][:-1]
            break


is_pretty_diff = False
is_interactive = True
current_config = []
updated_config = []
output = []

# Read the input files
with open('a.txt', 'r') as current:
    current_config = current.readlines()
with open('b.txt', 'r') as updated:
    updated_config = updated.readlines()

# Init the Differ engine
diff = Differ()
diff_result = list(diff.compare(current_config, updated_config))

# Iterate over the diff data to see which lines to include
for i in range(0, len(diff_result)):
    if diff_result[i].startswith(' '):
        # Always add common lines
        append(i)
    elif diff_result[i].startswith('-'):
        try:
            value_index = diff_result[i].index(':')
        except ValueError:
            # There is no : in this field, this must be an array entry
            # Add it
            append(i)
            continue
        # Find first occurence of ^-+ in diff hint
        change_index = diff_result[i+1].find('^')
        change_index = min_not_one(change_index,
                                   diff_result[i+1].find('+'))
        change_index = min_not_one(change_index,
                                   diff_result[i+1].find('-'))
        # Paramater name has changed, add this line
        if change_index != -1 and change_index > value_index:
            append(i)
            continue
        
        print
        print colours.BLUE + 'DELETION' + colours.ENDC
        show_context(i, '-')
        print '\nInclude in the new file?'
        print '[Y]es, [N]o'
        answer = str(raw_input())
        if (answer == 'y' or answer == 'Y'):
            append(i)
        """
        # Only add when this is a change, not a deletion
        if diff_result[i+1].startswith('-') or \
           (diff_result[i+1].startswith('+') and
           not diff_result[i+2].startswith('?')):
            continue
        else:
            append(i)
        """
    elif diff_result[i].startswith('+'):
        try:
            value_index = diff_result[i].index(':')
        except ValueError:
            # There is no : in this field, this must be an array entry
            # Omit it
            continue
        
        if i < len(diff_result) - 1:
            # Find first occurence of ^-+ in diff hint
            change_index = diff_result[i+1].find('^')
            change_index = min_not_one(change_index,
                                       diff_result[i+1].find('+'))
            change_index = min_not_one(change_index,
                                       diff_result[i+1].find('-'))
            # Paramater name has changed, add this line
            if change_index != -1 and change_index > value_index:
                continue

        print
        print colours.BLUE + 'ADDITION' + colours.ENDC
        show_context(i, '+')
        print '\nInclude in the new file?'
        print '[Y]es, [N]o'
        answer = str(raw_input())
        if answer == 'y' or answer == 'Y':
            append(i)        
        """
        # See if there is a diff hint
        if diff_result[i+1].startswith('?'):
            # See if parameter name changed
            try:
                value_index = diff_result[i].index(':')
            except ValueError:
                # There is no : in this field, this must be an array entry
                # Omit it
                continue
                
            # Find first occurence of ^-+ in diff hint
            change_index = diff_result[i+1].find('^')
            change_index = min_not_one(change_index,
                                       diff_result[i+1].find('+'))
            change_index = min_not_one(change_index,
                                       diff_result[i+1].find('-'))
            # Paramater name has changed, add this line
            if change_index != -1 and change_index < value_index:
                append(i)
        else:
            # Completely new line
            append(i)
        """
    else:
        # Don't add diff hints
        continue

# Check to see if we should print a nice diff version or plain output
if is_pretty_diff:
    pretty_print(ndiff(current_config, output))
else:
    print ''.join(output)
