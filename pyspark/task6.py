import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1])
lines = lines.mapPartitions(lambda x: reader(x))
# print(lines.collect())
lines = lines.map(lambda x: ((x[14],x[16]), 1)).reduceByKey(lambda x,y:x+y)
#print(lines.takeOrdered(20, lambda x: (-x[1], x[0][0])))
lines = sc.parallelize(lines.takeOrdered(20, lambda x: (-x[1],x[0][0])))
#print(type(lines.collect()) )
#for record in lines.collect():
#    print(str(record[0])+"\t"+str(record[1]))
# print(type(lines))
lines = lines.map(lambda x: (str(x[0][0])+", "+str(x[0][1])+"\t"+str(x[1])))
#print(lines.map(lambda x: x[0]).collect())
#lines = sc.parallelize(lines.take(20))
#print(sc.parallelize([lines.take(2)]))
#for each in lines.collect():
#    print(str(each))
lines.saveAsTextFile("task6.out")
