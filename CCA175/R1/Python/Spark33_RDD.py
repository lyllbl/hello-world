from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")


EmployeeNameRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeName.csv")

EmployeeNamePairRDD = EmployeeNameRDD.map(lambda line:line.split(',')).map(lambda words:(words[0],words[1]))

EmployeeSalaryRDD = sc.textFile("hdfs://master:9000/sparkData/input/EmployeeSalary.csv")

EmployeeSalaryPairRDD = EmployeeSalaryRDD.map(lambda line:line.split(',')).map(lambda words:(words[0],words[1]))

EmployeeNamePairRDDJoinEmployeeSalaryPairRDD = EmployeeNamePairRDD.join(EmployeeSalaryPairRDD)

#obtain the values of RDD

EmployeeNamePairRDDJoinEmployeeSalaryPairRDDValues = EmployeeNamePairRDDJoinEmployeeSalaryPairRDD.values()

SalaryNameRDD = EmployeeNamePairRDDJoinEmployeeSalaryPairRDDValues.map(lambda (name,salary):(salary,name))

SalaryGroupNameRDD = SalaryNameRDD.reduceByKey(lambda x,y: x+','+y)

print(SalaryGroupNameRDD.collect())
print(SalaryGroupNameRDD.keys().collect())
print(SalaryGroupNameRDD.values().collect())

for keyList in SalaryGroupNameRDD.keys().collect():
	print("Started"+keyList)
	print(SalaryGroupNameRDD.lookup(keyList))
	SalaryGroupNameRDD.filter(lambda lines:lines[0] == keyList).saveAsTextFile("hdfs://master:9000/sparkData/output/33/RDD/Salary"+keyList)
	print("Ended")
