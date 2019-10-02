import sys
from pyspark import SparkContext
from operator import add
from csv import reader
sc = SparkContext()
parking_violations = sc.textFile(sys.argv[1])
parking_violations = parking_violations.mapPartitions(lambda x: reader(x))
parking_violations = parking_violations.map(lambda x: (x[14],(x[16]),1)).reduceByKey(lambda x,y:(x[0],x[1]+y[1]))#.takeOrdered(20,lambda x:(-x[1][1],x[0]))
#parking_violations = sc.parallelize(parking_violations)
result = parking_violations.map(lambda x: (str(x[0])+", "+str(x[1][0])+"\t"+str(x[1][1])))
for resu in result.collect():
    print(str(resu))
#result.saveAsTextFile("task6.out")
