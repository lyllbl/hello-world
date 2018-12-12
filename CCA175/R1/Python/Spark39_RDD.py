from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")


spark16file1RDD = sc.textFile("hdfs://master:9000/sparkData/input/spark16file1.txt").map(lambda line:line.split(',')).map(lambda line:(line[0],(line[1],line[2])))

spark16file2RDD = sc.textFile("hdfs://master:9000/sparkData/input/spark16file2.txt").map(lambda line:line.split(',')).map(lambda line:(line[0],(line[1],line[2])))

joinRDD = spark16file1RDD.join(spark16file2RDD)

reduceRDD = joinRDD.map(lambda line:(1,line[1][0][1])).reduceByKey(lambda x,y:x+'+'+y).values()

print(joinRDD.collect())

print(reduceRDD.collect())



joinRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/39/RDD/Join")
reduceRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/39/RDD/Reduce")

