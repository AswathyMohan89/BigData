import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
#result = parking.groupBy("violation_code").count()
#result.select(format_string("%d",result.count)).show()
parking.createOrReplaceTempView("parking")
result = spark.sql("select violation_code, count(*) as ctr from parking group by violation_code")
result.select(format_string("%d\t%d",result.violation_code, result.ctr)).write.save("task2-sql.out", format="text")
