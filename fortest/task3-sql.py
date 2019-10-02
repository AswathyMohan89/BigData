import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum,  format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

open = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open.createOrReplaceTempView("open")
result = spark.sql("select license_type, round(avg(amount_due),2) as avg, round(sum(amount_due),2) as sum from open group by license_type")
result.select(format_string("%s\t%.2f, %.2f",result.license_type, result.sum, result.avg)).write.save("task3-sql.out", format="text")
