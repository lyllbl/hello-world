from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(["dog", "tiger", "lion", "cat", "spider", "eagle"], 2)
print(a.collect())

keyoutput = a.keyBy(lambda item:len(item))
print(keyoutput.collect())
output = keyoutput.groupByKey()
print(output.mapValues(list).collect())

#it looks likt groupBy() = keyBy().groupByKey()
groupBy = a.groupBy(lambda item:len(item))
print(groupBy.mapValues(list).collect())