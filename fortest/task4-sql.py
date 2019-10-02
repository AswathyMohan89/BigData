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
result = spark.sql("select p.registration, count(*) as ctr from (select case registration_state when 'NY' then 'NY' else 'Other' end as registration from parking) p group by p.registration order by p.registration")
result.select(format_string("%s\t%d",result.registration, result.ctr)).write.save("task4-sql.out", format="text")
