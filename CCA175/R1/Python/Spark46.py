from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")


list = [("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)]

columnList = [StructField("name",StringType()),StructField("sex",StringType()),StructField("cost",IntegerType())]

schemaType = StructType(columnList)

#listDF = spark.createDataFrame(list,schemaType) is also work
listDF = spark.createDataFrame(list,schema = schemaType)

print(listDF.show())

#method 1

listSumDF = listDF.groupBy("name","sex").sum("cost").withColumnRenamed("sum(cost)",'Total')

print(listSumDF.show())

#method 2
listDF.registerTempTable("PeopleCost")

listTableDF = spark.sql("select name,sex,sum(cost) as total_cost from PeopleCost group by name,sex")
print(listTableDF.show())


listTableDF.repartition(1).write.csv("hdfs://master:9000/sparkData/output/46/DataFrame", mode = 'overwrite', sep = ',', header = True)