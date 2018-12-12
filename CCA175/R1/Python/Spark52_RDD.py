from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

b = sc.parallelize([1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1])
print(b.collect())

#Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
count = b.countByValue()
#because return is a dictionary, then use items()
print(count.items())