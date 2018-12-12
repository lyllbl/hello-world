from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

salseDF = spark.read.csv("hdfs://master:9000/sparkData/input/sales.txt", header = True, inferSchema = True)

#agg(*exprs)
#If exprs is a single dict mapping from string to string, then the key is the column to perform aggregation on, and the value is the aggregate function.
#Alternatively, exprs can also be a list of aggregate Column expressions.

#method 1 ;could not alias immediately
#remember add {}, because that is dict object
salseSumAgg1DF = salseDF.groupby("Department","Designation","State").agg({"costToCompany":"sum","*":"count"})
print(salseSumAgg1DF.show())

#throw out TypeError: Column is not iterable
#salseSumDF = salseDF.groupby("Department","Designation","State").agg(sum(salseDF.costToCompany))

#method 2
#why need to add import functions and using F.sum or F.count
from pyspark.sql import functions as F
salseSumAgg2DF = salseDF.groupby("Department","Designation","State").agg(F.sum("costToCompany").alias("totalCost"),F.count("*").alias("empCount"))
print(salseSumAgg2DF.show())

#method 3; need to join, could not using one query to do sum and count together
salsesGroupByDF = salseDF.groupby("Department","Designation","State")
salsesCountDF = salsesGroupByDF.count().withColumnRenamed("count","empCount")
salsesSumDF = salsesGroupByDF.sum("costToCompany").withColumnRenamed("sum(costToCompany)","totalCost")

salsesJoinDF = salsesCountDF.join(salsesSumDF,["Department","Designation","State"],'outer')
salsesSelectDF = salsesJoinDF.select(salsesJoinDF.Department.alias("Dept"),salsesJoinDF.Designation.alias("Desg"),salsesJoinDF.State.alias("state"),salsesJoinDF.empCount,salsesJoinDF.totalCost)

print(salsesSelectDF.show())
salsesSelectDF.repartition(1).write.csv("hdfs://master:9000/sparkData/output/42/DataFrame",mode = 'overwrite',header = True)


#method 4 using spark Temp Table

salseDF.registerTempTable("salse")
DataDF = spark.sql("select Department AS Dept,Designation AS Desg,State,COUNT(1) AS empCount,SUM(costToCompany) AS totalCost from salse GROUP BY Department,Designation,State")
print("Spark SQL")
print(DataDF.show())
