from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

a = sc.parallelize(range(1,11), 3)

print(a.collect())

Output1 = a.filter(lambda item:item%2 == 0)

print(Output1.collect())

Output2 = a.filter(lambda item:item < 4)

print(Output2.collect())