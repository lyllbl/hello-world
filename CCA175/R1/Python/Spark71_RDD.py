from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

r1 = sc.textFile("hdfs://master:9000/sparkData/input/69Content.txt")
print(r1.collect())
r2 = r1.filter(lambda line:len(line)>0)
print(r2.collect())
r3 = r2.map(lambda line:(line.split(" ")[0],line))
print(r3.collect())
#r3.saveAsSequenceFile("hdfs://master:9000/sparkData/output/71/RDD/Seq/firstWord")

#null in python is None
r4 = r2.map(lambda line:(None,line))
print(r4.collect())
#r4.saveAsSequenceFile("hdfs://master:9000/sparkData/output/71/RDD/Seq/nullKey")

r5 = sc.sequenceFile("hdfs://master:9000/sparkData/output/71/RDD/Seq/nullKey")
for line in r5.collect():
	print(line)