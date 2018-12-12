from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

r1 = sc.textFile("hdfs://master:9000/sparkData/input/69Content.txt")
print(r1.collect())
r2 = r1.filter(lambda line:len(line)>0).flatMap(lambda line:line.split(" ")).filter(lambda word:len(word) > 2)
print(r2.collect())

for word in r2.collect():
	print(word)
r2.saveAsTextFile("hdfs://master:9000/sparkData/output/69/RDD")