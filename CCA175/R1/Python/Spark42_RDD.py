from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

salseRDD = sc.textFile("hdfs://master:9000/sparkData/input/sales.txt")
salseFirstRDD = salseRDD.first()

#remember that first() return is not RDD, just element
#remember to cast value to int.otherwise that is also string type
salseDataRDD = salseRDD.filter(lambda line: line not in salseFirstRDD).map(lambda line:line.split(',')).map(lambda line:((line[0],line[1],line[3]),(1,int(line[2]))))
salseReduceRDD = salseDataRDD.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]),numPartitions = 1)
print(salseReduceRDD.take(5))

salseReduceRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/42/RDD")