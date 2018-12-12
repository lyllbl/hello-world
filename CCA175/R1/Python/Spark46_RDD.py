from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

listData = [("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)]
listRDD = sc.parallelize(listData)

print(listRDD.collect())


listPairRDD = listRDD.map(lambda line:((line[0],line[1]),line[2]))

listReduceRDD = listPairRDD.reduceByKey(lambda x,y:x+y).map(lambda (k,v):(k[0],k[1],v))

print(listReduceRDD.collect())

listReduceRDD.repartition(1).saveAsTextFile("hdfs://master:9000/sparkData/output/46/RDD")