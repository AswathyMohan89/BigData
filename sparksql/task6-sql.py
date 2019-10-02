import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
#result = parking.groupBy("violation_code").count()
#result.select(format_string("%d",result.count)).show()
parking.createOrReplaceTempView("parking")
#spark.sql("select registration_state from parking").show()
result = spark.sql("select plate_id as plate, registration_state as registration, count(*) as ctr from parking group by plate_id, registration_state order by ctr desc, plate limit 20")
#result.createOrReplaceTempView("result")
#result = spark.sql("select registration, count(*) as ctr from result group by registration")
#result.printSchema()
#result.show()
result.select(format_string("%s, %s\t%d",result.plate, result.registration, result.ctr)).write.save("task6-sql.out", format="text")
