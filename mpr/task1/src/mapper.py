#!/usr/bin/env python

import sys
import os
import string

for line in sys.stdin:
    filename = ''
    filename = os.environ['map_input_file']
    origin = 0
    line = line.strip()
    words = line.split(',')
    issue_date = None
    violation_code = None
    violation_precinct = None
    plate_id = None
    if 'parking' in filename:
        issue_date = words[1]
        violation_code = words[2]
        violation_precinct = words[6]
        plate_id = words[14]
        origin = 1
    elif 'open' in filename :
        origin = 2
    w = words[0]
    if w:
        list_to_return=[]
        list_to_return.append(origin)
        list_to_return.append(plate_id)
        list_to_return.append(violation_precinct)
        list_to_return.append(violation_code)
        list_to_return.append(issue_date)
        print(w+"\t"+str(list_to_return))


