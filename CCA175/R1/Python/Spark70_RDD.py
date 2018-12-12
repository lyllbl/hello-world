from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

r1 = sc.textFile("hdfs://master:9000/sparkData/input/69Content.txt")
print(r1.collect())
r2 = r1.filter(lambda line:len(line)>0).flatMap(lambda line:line.split(" "))
print(r2.collect())
r3 = r2.map(lambda word:(word,1)).reduceByKey(lambda x,y:x + y)
print(r3.collect())

r3.saveAsTextFile("hdfs://master:9000/sparkData/output/70/RDD")