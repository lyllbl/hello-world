from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

employee2 = spark.sql("select * from sparkdb.employee2")

print(employee2.show())

print("All record and column")
for row in employee2.collect():
	print(row)

print("one column")
for row in employee2.collect():
	print(row.last_name)
