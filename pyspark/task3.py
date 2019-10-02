import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
# print(lines.collect())
lines = lines.map(lambda x: (x[2],(1,float(x[12])))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]) )
#print(lines.collect())
# for record in lines.collect():
#    print(str(record[0])+"\t"+str(record[1]))
# print(type(lines))
lines = lines.map(lambda x: ("%s\t%.2f, %.2f"%(x[0], x[1][1], x[1][1]/x[1][0] )))
#print(lines.collect())
lines.saveAsTextFile("task3.out")
