import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking_violations = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open_violations = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

parking_violations.createOrReplaceTempView("parking_violations")
open_violations.createOrReplaceTempView("open_violations")

result = spark.sql("select pv.summons_number as s, pv.plate_id as p, pv.violation_precinct as vp, pv.violation_code as vc, pv.issue_date from parking_violations pv where pv.summons_number not in (select ov.summons_number from open_violations ov)")

#result.select(format_string("%s\t%s, %s, %s, %s",result.summons_number, result.plate_id, result.violation_precinct, result.violation_code, date_format(result.issue_date, 'yyyy-MM-dd'))).write.save("task1-sunny.out", format="text")

result.select(format_string("%s\t%s, %s, %s, %s",result.s, result.p, result.vp, result.vc, date_format(result.issue_date, 'yyyy-MM-dd'))).write.save("task1-sunny.out", format="text")
