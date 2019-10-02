import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1])
lines = lines.mapPartitions(lambda x: reader(x))
lines = lines.map(lambda x: ((x[14],x[16]), 1)).reduceByKey(lambda x,y:x+y)
lines = sc.parallelize(lines.takeOrdered(20, lambda x: (-x[1],x[0][0])))
lines = lines.map(lambda x: (str(x[0][0])+", "+str(x[0][1])+"\t"+str(x[1])))
lines.saveAsTextFile("task6.out")
