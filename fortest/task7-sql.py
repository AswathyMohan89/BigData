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
#weekend_dates=[(5,),(6,),(12,),(13,),(19,),(20,),(26,),(27,)]
weekend_dates=[("2016-03-05",),("2016-03-06",),("2016-03-12",),("2016-03-13",),("2016-03-19",),("2016-03-20",),("2016-03-26",),("2016-03-27",)]
weekend = spark.createDataFrame(weekend_dates, ["date"])
weekend.createOrReplaceTempView("weekend")
#result = spark.sql("select violation_code, round(sum(case when w.date is null then 1 else 0 end)/23,2) as wkd, round(count(w.date)/8,2) as ctr_weekend from parking p left outer join weekend w on extract(DAY from parking.issue_date)=w.date group by violation_code")
result = spark.sql("select violation_code, round(sum(case when w.date is null then 1 else 0 end)/23,2) as wkd, round(count(w.date)/8,2) as ctr_weekend from parking p left outer join weekend w on issue_date=w.date group by violation_code")
result.select(format_string("%s\t%.2f, %.2f",result.violation_code, result.ctr_weekend, result.wkd)).write.save("task7-sql.out", format="text")
