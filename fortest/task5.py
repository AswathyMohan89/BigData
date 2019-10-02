import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
lines = lines.map(lambda x: (x[14],(1,x[16]))).reduceByKey(lambda x,y:(x[0]+y[0],x[1])).sortBy(lambda x: x[1][0], ascending=False)
lines = lines.map(lambda x: (str(x[0])+", "+str(x[1][1])+"\t"+str(x[1][0])))
lines = sc.parallelize(lines.take(1))
lines.saveAsTextFile("task5.out")
