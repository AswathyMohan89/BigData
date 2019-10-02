import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
lines = lines.map(lambda x: (x[2],(1,0) if int(x[1][-2:]) in [5,6,12,13,19,20,26,27] else (0,1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
lines = lines.map(lambda x: ( str(x[0])+"\t"+str(round(x[1][0]/8,2))+", "+str(round(x[1][1]/23,2) ) ))
lines.saveAsTextFile("task7.out")
