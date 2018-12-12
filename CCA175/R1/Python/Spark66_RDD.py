from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "tiger", "lion", "cat", "spider", "eagle"], 2)
b = a.keyBy(lambda item:len(item))
c = sc.parallelize(["ant", "falcon", "squid"], 2)
d = c.keyBy(lambda item:len(item))
print(b.collect())
print(d.collect())

output = b.subtractByKey(d)
print(output.collect())