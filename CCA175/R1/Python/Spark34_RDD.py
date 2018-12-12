from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

userRDD = sc.textFile("hdfs://master:9000/sparkData/input/user.csv")

userRDDFirst = userRDD.first()

userFilterRDD = userRDD.filter(lambda line: line != userRDDFirst).filter(lambda line: "myself" not in line)

resultRDD = userFilterRDD.map(lambda line:line.split(',')).map(lambda line:("id->"+line[0]+','+"topic->"+line[1]+','+"hits->"+line[2]))

resultRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/34/RDD")