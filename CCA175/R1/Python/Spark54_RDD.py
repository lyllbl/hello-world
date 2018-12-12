from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", "eagle"])
print(a.collect())

b = a.map(lambda item:(len(item),item))
print(b.collect())

output = b.foldByKey('',lambda x,y:x+""+y)

print(output.collect())