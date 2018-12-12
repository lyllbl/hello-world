from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

dataRDD = sc.textFile("hdfs://master:9000/sparkData/input/data.csv")

dataPairRDD = dataRDD.map(lambda line:line.split(','))

dataPairReduceRDD = dataPairRDD.reduceByKey(lambda x,y:(x+','+y))


#tuple must need to be added
dataPairReduceTupleRDD = dataPairReduceRDD.map(lambda (k,v):(k,tuple(v.split(','))))

dataPairReduceTupleRDD.saveAsTextFile("hdfs://master:9000/sparkData/output/36/RDD")