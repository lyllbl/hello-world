from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")


ContentRDD = sc.textFile("hdfs://master:9000/sparkData/input/Content.txt")

print(ContentRDD.take(5))


removeRDD = sc.textFile("hdfs://master:9000/sparkData/input/Remove.txt")

print(removeRDD.take(5))


removeRDDBroadcast = sc.broadcast(removeRDD.flatMap(lambda lines:lines.split(',')).map(lambda word:word.strip().lower()).collect())

print(removeRDDBroadcast.value)


ContentRDDRemoveRDD = ContentRDD.flatMap(lambda lines:lines.split(' ')).filter(lambda words:words.lower() not in removeRDDBroadcast.value)

print(ContentRDDRemoveRDD.collect())

ContentRDDRemoveRDDCount = ContentRDDRemoveRDD.map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

print(ContentRDDRemoveRDDCount.collect())

ContentRDDRemoveRDDCount.saveAsTextFile("hdfs://master:9000/sparkData/output/31/RDD/result")

