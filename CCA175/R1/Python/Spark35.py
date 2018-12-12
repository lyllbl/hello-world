from pyspark.sql import SparkSession

from pyspark.sql.types import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

EmployeeNameColumns = [StructField("id",StringType()),StructField("name",StringType())]

EmployeeNameSchema = StructType(EmployeeNameColumns)

EmployeeNameDF = spark.read.csv("/sparkData/input/EmployeeName.csv",schema = EmployeeNameSchema)

print(EmployeeNameDF.show())

EmployeeNameDFSort = EmployeeNameDF.orderBy("name",ascending = False)

print(EmployeeNameDFSort.show())

EmployeeNameDFSort.repartition(1).write.csv("hdfs://master:9000/sparkData/output/35/DataFrame")