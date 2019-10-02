import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
#print(lines.collect())
lines = lines.map(lambda x: (x[2],(1,0) if int(x[1][-2:]) in [5,6,12,13,19,20,26,27] else (0,1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
lines = lines.map(lambda x: ( str(x[0])+"\t"+str(round(x[1][0]/8,2))+", "+str(round(x[1][1]/23,2) ) ))
for each in lines.collect():
    print(str(each))
# for record in lines.collect():
#    print(str(record[0])+"\t"+str(record[1]))
# print(type(lines))
#lines = lines.map(lambda x: ( str(x[0])+"\t"+str(round(x[1][1],2))+", "+str(round(x[1][1]/x[1][0],2) ) ))
#print(lines.collect())
lines.saveAsTextFile("task7.out")
