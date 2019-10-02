#!/usr/bin/env python

from operator import itemgetter
import sys

current_code = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    # parse the input we got from mapper.py
    violation_code, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue

    if current_code == violation_code:
        current_count += count
    else:
        if current_code:
            # write result to STDOUT
            print(str(current_code)+"\t"+str(current_count))
        current_count = count
        current_code = violation_code

if current_code == violation_code:
    print(str(current_code)+"\t"+str(current_count))
