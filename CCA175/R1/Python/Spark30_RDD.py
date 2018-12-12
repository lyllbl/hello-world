from pyspark.sql import SparkSession


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

EmployeeManagerRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeManager.csv")

print(EmployeeManagerRDD.take(5))

EmployeeNameRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeName.csv")

print(EmployeeNameRDD.take(5))

EmployeeSalaryRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeSalary.csv")

print(EmployeeSalaryRDD.take(5))


EmployeeNamePariRDD = EmployeeNameRDD.map(lambda line:line.split(',')).map(lambda ListLine:(ListLine[0],ListLine[1]))
EmployeeSalaryPariRDD = EmployeeSalaryRDD.map(lambda line:line.split(',')).map(lambda ListLine:(ListLine[0],ListLine[1]))
EmployeeManagerPariRDD = EmployeeManagerRDD.map(lambda line:line.split(',')).map(lambda ListLine:(ListLine[0],ListLine[1]))

print (EmployeeNamePariRDD.take(5))
print (EmployeeSalaryPariRDD.take(5))
print (EmployeeManagerPariRDD.take(5))

EmployeeNamePariRDDJoinEmployeeSalaryPariRDD = EmployeeNamePariRDD.join(EmployeeSalaryPariRDD)

print (EmployeeNamePariRDDJoinEmployeeSalaryPariRDD.take(5))

EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDD = EmployeeNamePariRDDJoinEmployeeSalaryPariRDD.join(EmployeeManagerPariRDD)

print (EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDD.take(5))

EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDDSorted = EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDD.sortByKey()

print (EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDDSorted.take(5))

ResultData = EmployeeNamePariRDDJoinEmployeeSalaryPariRDDJoinEmployeeManagerPariRDDSorted.map(lambda lines: (lines[0],lines[1][0][0],lines[1][0][1],lines[1][1]))

print(ResultData.take(5))

ResultData.saveAsTextFile("hdfs://master:9000/sparkData/output/30/RDD/ResultData")
