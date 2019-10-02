#!/usr/bin/env python

from operator import itemgetter
import sys
current_summons = None
current_origin = []
summons_number = None
origin = []
for line in sys.stdin:
    summons_number, details_string = line.split('\t', 1)
    try:
        details_string = details_string[1:len(details_string)-2]
        #details = details_string.split(", ")
        origin = int(details_string[0])
        #list_of_details = [detail.strip("'") for detail in details[1:]]
    except ValueError:
        continue
    if current_summons == summons_number:
        current_origin.append(origin)
    else:
        if len(current_origin)==1 and current_origin[0]==1:
            print(current_summons+"\t"+str(list_of_details[0]) +", " + str(list_of_details[1]) +", "+str(list_of_details[2]) +", "+ str(list_of_details[3]))
        current_origin = []
        details_string = details_string[1:len(details_string)-1]
        details = details_string.split(", ")
        list_of_details = [detail.strip("'") for detail in details[1:]]
        current_origin.append(origin)
        current_summons = summons_number

if len(current_origin)==1 and current_origin[0]==1:
            print(current_summons+"\t"+str(list_of_details[0]) +", " + str(list_of_details[1]) +", "+str(list_of_details[2]) +", "+ str(list_of_details[3]))
