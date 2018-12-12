from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

pairRDD1 = sc.parallelize([("cat",2), ("cat", 5), ("book", 4),("cat", 12)])
pairRDD2 = sc.parallelize([("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)])

DF1 = spark.createDataFrame(pairRDD1,"name:string,num:int")
DF2 = spark.createDataFrame(pairRDD2,"name:string,num:int")

print(DF1.show())
print(DF2.show())

output = DF1.join(DF2,'name','full_outer')

print(output.show())