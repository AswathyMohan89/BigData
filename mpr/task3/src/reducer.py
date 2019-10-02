#!/usr/bin/env python

from operator import itemgetter
import sys

current_type = None
current_count = 0
current_total = 0.00
word = None

# input comes from STDIN
for line in sys.stdin:
    license_type, details_string = line.split('\t', 1)
    # print(license_type, details_string)
    
    try:
        details_string = details_string[1:len(details_string)-2]
        details = details_string.split(", ")
        count = int(details[0])
        total = float(details[1])
    except ValueError:
        continue
    
    if current_type == license_type:
        current_count += count
        current_total += total
    else:
        if current_type:
            print(str(current_type)+"\t"+str(current_count)+", "+str(current_total)+", "+str(current_total/current_count))
        current_count = 1
        # details_string = details_string[1:len(details_string)-2]
        # print(details_string)
        # details = details_string.split(", ")
        # list_of_details = [detail.strip("'") for detail in details[1:]]
        # print(list_of_details)
        current_total = total
        current_type = license_type
if current_type == license_type:
    print(str(current_type)+"\t"+str(current_count)+", "+str(current_total)+", "+str(current_total/current_count))
