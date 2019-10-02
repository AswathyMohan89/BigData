import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum,  format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

open = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
#open.select(open["license_type"], open["amount_due"]).groupBy("license_type").count().show()
#open.groupBy("license_type").agg(avg("amount_due"), sum("amount_due")).show()
#result = open.groupBy("license_type").count()
#result.select(format_string("%d",result.count)).show()
open.createOrReplaceTempView("open")
result = spark.sql("select license_type, round(avg(amount_due),2) as avg, round(sum(amount_due),2) as sum from open group by license_type")
#result.printSchema()
#result = result.select(result.license_type, result.sum.cast("float"), result.avg.cast("float"))
result.select(format_string("%s\t%.2f, %.2f",result.license_type, result.sum, result.avg)).write.save("task3-sql.out", format="text")
