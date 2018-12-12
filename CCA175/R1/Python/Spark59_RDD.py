from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

x = sc.parallelize(range(1,21))
y = sc.parallelize(range(10,31))

print(x.collect())
print(y.collect())

output = x.intersection(y)

print(output.collect())

output1 = x.subtract(y)
print(output1.collect())