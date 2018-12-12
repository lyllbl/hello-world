from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "salmon", "salmon", "rat", "elephant"], 3)
b = a.keyBy(lambda item:len(item))
print(b.collect())
c = sc.parallelize(["dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"], 3)
d = c.keyBy(lambda item:len(item))
print(d.collect())

output = b.rightOuterJoin(d)
print(output.collect())
