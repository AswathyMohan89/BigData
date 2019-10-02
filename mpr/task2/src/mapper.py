#!/usr/bin/env python

import sys
import string
import numpy

for line in sys.stdin:
    line = line.strip()
    # split the line into words
    words = line.split(",")
    violation_code = words[2]
    # print(violation_code)
    try:
        violation_code = int(violation_code)
    # increase counters
        if violation_code:
            print(str(violation_code)+"\t"+str(1))
    except ValueError:
        continue
    # for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
     #  w=word.strip(string.punctuation)
     #  if w:
     #     print(w.lower(), 1)


