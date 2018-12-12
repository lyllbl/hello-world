from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

pairRDD1 = sc.parallelize([("cat",2), ("cat", 5), ("book", 4),("cat", 12)])
pairRDD2 = sc.parallelize([("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)])

output = pairRDD1.fullOuterJoin(pairRDD2)

print(output.collect())