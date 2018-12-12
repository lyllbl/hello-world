from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

employee = spark.read.json("hdfs://master:9000/sparkData/input/employee.json")

print(employee.show())

employee.registerTempTable("employee")

r1 = spark.sql("select * from employee")
print(r1.show())

#toJSON return is a RDD
r1.toJSON().saveAsTextFile("hdfs://master:9000/sparkData/output/73/RDD")
r1.write.json("hdfs://master:9000/sparkData/output/73/DataFrame", mode = 'overwrite')