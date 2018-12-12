from pyspark.sql import SparkSession;
from pyspark.sql.types import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate();
sc = spark.sparkContext

sc.setLogLevel("ERROR")

EmployeeNameColumns = [StructField("id",StringType()),StructField("name",StringType())]
EmployeeNameSchema = StructType(EmployeeNameColumns)

EmployeeName = spark.read.csv("hdfs://master:9000/sparkData/input/EmployeeName.csv", schema = EmployeeNameSchema)

#print(EmployeeName.show())

EmployeeSalaryColumns = [StructField("id",StringType()),StructField("Salary",IntegerType())]
EmployeeSalarySchema = StructType(EmployeeSalaryColumns)

EmployeeSalary = spark.read.csv("hdfs://master:9000/sparkData/input/EmployeeSalary.csv", schema = EmployeeSalarySchema)

#print(EmployeeSalary.show())

EmployeeNameJoinEmployeeSalary = EmployeeName.join(EmployeeSalary,"id","outer")

#print(EmployeeNameJoinEmployeeSalary.show())

SalaryName = EmployeeNameJoinEmployeeSalary.select("Salary","name")

#print(SalaryName.show())

SalaryDistinct = SalaryName.select("salary").distinct()

#print(SalaryDistinct.show())

for row in SalaryDistinct.collect():
	print(row)
	print(row.salary)
	NameList = SalaryName.where("Salary == "+str(row.salary))
	print(NameList.collect())
	NameList.write.format("csv").save("hdfs://master:9000/sparkData/output/33/DataFrame/Salary"+str(row.salary))