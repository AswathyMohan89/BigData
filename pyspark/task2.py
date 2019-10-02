import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
# print(lines.collect())
lines = lines.map(lambda x: (x[2],1)).reduceByKey(lambda x,y:x+y)
#print(type(lines.collect()) )
#for record in lines.collect():
#    print(str(record[0])+"\t"+str(record[1]))
# print(type(lines))
lines = lines.map(lambda x: (str(x[0])+"\t"+str(x[1])))
# print(lines.collect())
lines.saveAsTextFile("task2.out")
