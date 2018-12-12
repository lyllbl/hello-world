from pyspark.sql import SparkSession;
from pyspark.sql.types import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate();
sc = spark.sparkContext

sc.setLogLevel("ERROR")

EmployeeManagerColums = [StructField("id",StringType()),StructField("manager_Name",StringType())]
EmployeeManagerSchema = StructType(EmployeeManagerColums)

EmployeeManager = spark.read.csv("hdfs://master:9000/sparkData/input/EmployeeManager.csv",schema = EmployeeManagerSchema)

print(EmployeeManager.show())

EmployeeNameColumns = [StructField("id",StringType()),StructField("name",StringType())]
EmployeeNameSchema = StructType(EmployeeNameColumns)

EmployeeName = spark.read.csv("hdfs://master:9000/sparkData/input/EmployeeName.csv", schema = EmployeeNameSchema)

print(EmployeeName.show())

EmployeeSalaryColumns = [StructField("id",StringType()),StructField("Salary",IntegerType())]
EmployeeSalarySchema = StructType(EmployeeSalaryColumns)

EmployeeSalary = spark.read.csv("hdfs://master:9000/sparkData/input/EmployeeSalary.csv", schema = EmployeeSalarySchema)

print(EmployeeSalary.show())

EmployeeNameJoinEmployeeSalary = EmployeeName.join(EmployeeSalary,'id','outer')

print(EmployeeNameJoinEmployeeSalary.show())

EmployeeNameJoinEmployeeSalaryJoinEmployeeManager = EmployeeNameJoinEmployeeSalary.join(EmployeeManager,'id','outer')

print(EmployeeNameJoinEmployeeSalaryJoinEmployeeManager.show())


EmployeeNameJoinEmployeeSalaryJoinEmployeeManagerSort = EmployeeNameJoinEmployeeSalaryJoinEmployeeManager.sort("id",ascending=False)

print(EmployeeNameJoinEmployeeSalaryJoinEmployeeManagerSort.show())


EmployeeNameJoinEmployeeSalaryJoinEmployeeManagerSort.write.csv("hdfs://master:9000/sparkData/output/30/DataFrame",mode = "overwrite", sep = ",", header = "True")
