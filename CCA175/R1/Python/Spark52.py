from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

b = sc.parallelize([1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1])

#Could not use StructType as schema, throw out TypeError: StructType can not accept object 1 in type <type 'int'>
#aDF = spark.createDataFrame(b,"num:int")
#print(aDF.show())

bDF = spark.createDataFrame(b,IntegerType())

print(bDF.show())

column=Row("Num")
DF = b.map(column).toDF()
print(DF.show())

result1DF = DF.groupby("Num").count()
print(result1DF.show())



