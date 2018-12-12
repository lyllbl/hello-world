from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(range(1,101),3)
print(a.collect())

print(a.glom())

output = a.glom().collect()
print(output)