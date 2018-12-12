from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")


spark15file1DF = spark.read.csv("hdfs://master:9000/sparkData/input/spark15file1.txt", sep = ',')

#Could not use replace,because replace(to_replace, value=<no value>, subset=None),to_replace could no be none
#spark15file1NoZeroDF = spark15file1DF.replace(None,'0')
spark15file1NoZeroDF = spark15file1DF.fillna('0')

print(spark15file1DF.show())
print(spark15file1NoZeroDF.show())

spark15file1NoZeroDF.write.csv("hdfs://master:9000/sparkData/output/40/DaraFrame", mode = 'overwrite', sep = ',')

#directly using nullValue = '0' in write funcation
spark15file1DF.write.csv("hdfs://master:9000/sparkData/output/40/DaraFrameNullVale", mode = 'overwrite',sep = ',', nullValue = '0')