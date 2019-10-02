#!/usr/bin/env python

import sys
import string
import numpy
from csv import reader
for line in reader(sys.stdin):
    # line = line.strip()
    # split the line into words
    #words = line.split(",")
    words = line
    license_type = words[2]
    total_amount = float(words[12]) #+float(words[13])+float(words[14])+float(words[15])
    try:
        if license_type:
            print(str(license_type)+"\t"+str([1,total_amount]))
    except ValueError:
        continue
     

