import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL assignment") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
parking.createOrReplaceTempView("parking")
open.createOrReplaceTempView("open")
result = spark.sql("select p.summons_number as summons, p.plate_id as plate, p.violation_precinct as precinct, p.violation_code as code, p.issue_date as date from parking p left join open o on p.summons_number=o.summons_number where o.summons_number is null")
result.select(format_string("%s\t%s, %s, %s, %s",result.summons, result.plate, result.precinct, result.code, date_format(result.date, 'yyyy-MM-dd'))).write.save("task1-sql.out", format="text")
