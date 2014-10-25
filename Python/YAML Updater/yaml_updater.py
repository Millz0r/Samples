#!/usr/bin/env python
# yaml interactive config updater
# Karol Pogonowski
# Usage:
# ./yaml_updater.py <current_config> <updated_config> [output_file] [-d]
import sys
import yaml
from difflib import ndiff


class Colours:
    """Console colour codes"""
    GREY = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'


def console_text(colour, text):
    """Enclose text in console markers"""
    return colour + text + Colours.ENDC


def pretty_print(diff_text):
    """Enclose diffs in specific console Colours codes"""
    for item in diff_text:
        if item.startswith('+'):
            print console_text(Colours.GREEN, item)
        elif item.startswith('-'):
            print console_text(Colours.RED, item)
        else:
            print console_text(Colours.GREY, item)


def show_context(key, mode):
    """Prints the context around mode-specific diff line."""
    # Set environment according to mode
    if mode == '-':
        datafile = current_file
        colour = Colours.RED
    else:
        datafile = updated_file
        colour = Colours.GREEN
    # Find that line
    for i in range(0, len(datafile)):
        if datafile[i].startswith(key):
            break
    # Print the altered line in specific colour
    print
    if i > 0:
        print datafile[i-1][:-1]
    print console_text(colour, datafile[i][:-1])
    if i+1 < len(datafile):
        print datafile[i+1][:-1]


def interactive(key, value, mode):
    """Handles user input for conflict resolution"""
    global output_config
    global comment_buffer
    # Show context around the offending line first
    if mode == '-':
        print console_text(Colours.BLUE, 'DELETION')
        show_context(key, mode)
    else:
        print console_text(Colours.BLUE, 'ADDITION')
        show_context(key, mode)

    # Display options
    print '\n\nKeep the line in new file?'
    if mode == '+':
        print console_text(Colours.BOLD, '[Y]es') + ', [N]o, [C]omment out'
    else:
        print '[Y]es, [N]o, ' + console_text(Colours.BOLD, '[C]omment out')
    answer = str(raw_input())

    # Omit
    if (answer == 'n' or answer == 'N'):
        if mode == '-':
            del output_config[key]
    # Add, default for new lines
    elif (answer == 'y' or answer == 'Y') or \
           (not (answer == 'c' or answer == 'C') and mode == '+'):
        output_config[key] = value
    # Comment out, the default option
    else:
        if mode == '-':
            del output_config[key]
        comment_buffer += '# ' + str(key) + ': ' + str(value) + '\n'


is_pretty_diff = False
current_config = []
updated_config = []
current_file = ''
current_filename = ''
updated_file = ''
updated_filename = ''
output_file = ''
output = ''
comment_buffer = ''

# Read script arguments
if len(sys.argv) < 3:
    print 'Usage:'
    print './script.py <current_config> <updated_config> [output_file] [-d]'
    sys.exit(0)
elif len(sys.argv) == 3:
    current_filename = sys.argv[1]
    updated_filename = sys.argv[2]
elif len(sys.argv) == 4:
    if '-d' in sys.argv[3]:
        is_pretty_diff = True
    else:
        output_file = sys.argv[3]
elif len(sys.argv) == 5:
    output_file = sys.argv[3]
    if '-d' in sys.argv[4]:
        is_pretty_diff = True

# Read the input files
with open(current_filename, 'r') as current:
    current_config = yaml.safe_load(current)
with open(current_filename, 'r') as current:
    output_config = yaml.safe_load(current)
with open(current_filename, 'r') as current:
    current_file = current.readlines()
with open(updated_filename, 'r') as updated:
    updated_config = yaml.safe_load(updated)
with open(updated_filename, 'r') as updated:
    updated_file = updated.readlines()

# Loop through parsed values and see which ones differ
for current in current_config.keys():
    if current not in updated_config.keys():
        # Deleted line
        interactive(current, current_config[current], '-')
for updated in updated_config.keys():
    if updated not in current_config.keys():
        # Added line
        interactive(updated, updated_config[updated], '+')

# Serialize the YAML output
output = yaml.safe_dump(output_config, default_flow_style=False).splitlines()

# Process comments
for key in output_config.keys():
    index = -1
    # Look in the current config first
    line_buffer = current_file
    for line in line_buffer:
        if line.startswith(key):
            index = line_buffer.index(line)
            break
    # Didn't find, look in the other file
    if index == -1:
        line_buffer = updated_file
        for line in line_buffer:
            if line.startswith(key):
                index = line_buffer.index(line)
                break
    # Find where the comment ends
    for i in range(index, -1, -1):
        if line_buffer[i-1].startswith('#'):
            continue
        else:
            break
    # Find position within the output string
    for pos in range(0, len(output)):
        if output[pos].startswith(key):
            break
    output.insert(pos, '\n' + ''.join(line_buffer[i:index])[:-1])

# Fold the list on newlines and add one at the end
output = '\n'.join(output) + '\n'

# See if we should add commented-out lines
if comment_buffer != '':
    output += '\n# =========================\n'
    output += '# DELETED CONFIG PARAMETERS'
    output += '\n# =========================\n'
    output += comment_buffer

# Present output
print
print '=========================================='
print 'OUTPUT'
print '=========================================='
print

# Write to auxiliary file if specified
if output_file:
    with open(output_file, 'w') as out_file:
        out_file.writelines(output)

if is_pretty_diff:
    pretty_print(ndiff(yaml.safe_dump(current_config).splitlines(),
                       yaml.safe_dump(output_config).splitlines()))
else:
    print output
