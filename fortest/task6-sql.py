import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL assignment") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result = spark.sql("select plate_id as plate, registration_state as registration, count(*) as ctr from parking group by plate_id, registration_state order by ctr desc, plate limit 20")
result.select(format_string("%s, %s\t%d",result.plate, result.registration, result.ctr)).write.save("task6-sql.out", format="text")
