import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
lines = lines.map(lambda x: (x[2],(1,float(x[12])))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]) )
lines = lines.map(lambda x: ("%s\t%.2f, %.2f"%(x[0], x[1][1], x[1][1]/x[1][0] )))
lines.saveAsTextFile("task3.out")
