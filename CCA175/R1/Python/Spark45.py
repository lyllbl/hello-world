from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

technologyDF = spark.read.csv("hdfs://master:9000/sparkData/input/technology.txt", sep =',' , header = True, inferSchema = True)
salaryDF = spark.read.csv("hdfs://master:9000/sparkData/input/salary.txt", sep =',' , header = True, inferSchema = True)

resultJoinDF = technologyDF.join(salaryDF,['first','last'],'outer')

print(resultJoinDF.show())

resultJoinDF.repartition(1).write.csv("hdfs://master:9000/sparkData/output/45/DataFrame", mode = 'overwrite', header = True)
