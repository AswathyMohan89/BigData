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
#spark.sql("select registration_state from parking").show()
result = spark.sql("select p.registration, count(*) as ctr from (select case registration_state when 'NY' then 'NY' else 'Other' end as registration from parking) p group by p.registration order by p.registration")
#result.createOrReplaceTempView("result")
#result = spark.sql("select registration, count(*) as ctr from result group by registration")
#result.printSchema()
#result.show()
result.select(format_string("%s\t%d",result.registration, result.ctr)).write.save("task4-sql.out", format="text")
