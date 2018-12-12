from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", "eagle"], 2)
b = a.map(lambda item:(len(item),item))
print(b.collect())

output =  b.mapValues(lambda item:"x"+item+"x")

print(output.collect())