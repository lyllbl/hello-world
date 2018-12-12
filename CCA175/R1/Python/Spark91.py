from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

employee = spark.read.json("hdfs://master:9000/sparkData/input/employee.json")
print(employee.printSchema())
print(employee.show())
employee.registerTempTable("employee")
output = spark.sql("select * from employee")
print(output.show())
output.write.json("hdfs://master:9000/sparkData/output/91/json", mode = 'overwrite')