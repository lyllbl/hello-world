from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

technologyrRDD = sc.textFile("hdfs://master:9000/sparkData/input/technology.txt").map(lambda line:line.split(',')).map(lambda line:((line[0],line[1]),line[2]))
salaryRDD = sc.textFile("hdfs://master:9000/sparkData/input/salary.txt").map(lambda line:line.split(',')).map(lambda line:((line[0],line[1]),line[2]))

joinRDD = technologyrRDD.join(salaryRDD)

print(joinRDD.collect())

joinRDD.repartition(1).saveAsTextFile("hdfs://master:9000/sparkData/output/45/RDD")