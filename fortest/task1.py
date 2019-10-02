import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines1 = sc.textFile(sys.argv[1])
lines1 = lines1.mapPartitions(lambda x: reader(x))
lines2 = sc.textFile(sys.argv[2])
lines2 = lines2.mapPartitions(lambda x: reader(x))
lines1 = lines1.map(lambda x: (x[0],(x[14],x[6],x[2],x[1])))
lines2 = lines2.map(lambda x: (x[0],1))
result = lines1.subtractByKey(lines2)
result = result.map(lambda x: str(x[0])+"\t"+str(x[1][0])+", "+str(x[1][1])+", "+str(x[1][2])+", "+str(x[1][3]))
result.saveAsTextFile("task1.out")
