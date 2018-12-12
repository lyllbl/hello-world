from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(range(1,10), 3)
print(a.collect())

output = a.groupBy(lambda item: "even" if (item%2 == 0) else "odd" )

print(output.mapValues(list).collect())